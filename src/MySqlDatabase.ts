import config from 'config';
import createDebug from 'debug';
import mysql, { Connection, MysqlError, Query, queryCallback } from 'mysql';
import { DbError, DbErrorCode } from './DbError';

const debug = createDebug('westlakelabs:database');

// See: https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-error-sqlstates.html
enum SqlStateValue {
  NotFound = '02000',
  AccessDenied = '28000',
  DuplicateKey = '23000',
  GeneralError = 'HY000',
  UserDefiniedError = '45000'
}

// See: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
enum MySqlErrNo {
  ER_SIGNAL_NOT_FOUND = 1643,
  ER_ACCESS_DENIED_ERROR = 1045,
  ER_DUP_KEY = 1022,
  ER_WRONG_VALUE = 1525
}

/**
 * Configuration information for the database.
 */
export interface IDatabaseConfig {
  host: string;
  user: string;
  password: string;
  name: string;
}

const dbConfig = config.get<IDatabaseConfig>('Database');
debug(
  `Connecting to database ${dbConfig.name} on host ${dbConfig.host} as user ${dbConfig.user}.`
);
const connectionPool = mysql.createPool({
  connectionLimit: 20,
  host: dbConfig.host,
  user: dbConfig.user,
  password: dbConfig.password,
  database: dbConfig.name,
  // We store dates using the DATETIME type which has no
  // timezone information in MySQL.  The dates are provided
  // to MySQL in UTC.  When we get them back from the database
  // we don't want any timezone translation to occur so we
  // configure the mysql client with timezone='Z'.
  timezone: 'Z',

  // We use the DECIMAL type to store GPS coordinates.
  supportBigNumbers: true,

  // It would be nice to set bigNumberStrings to true as well so that
  // we don't have to worry about precision loss, but it affects count
  // values as well and we want those to be numbers.
  bigNumberStrings: false
});

/**
 * Class which exposes common database operations with promise-based results.
 */
export class MySqlDatabase {
  /**
   * Executes a database query.
   *
   * @param conn Database connection to use.
   * @param options Query to execute.
   * @param values Parameter values  to provide to the query.
   * @param callback Function to call with results.
   */
  protected query(
    conn: Connection,
    options: string,
    values: any,
    callback?: queryCallback
  ): Query {
    return conn.query(options, values, callback);
  }

  /**
   * Invokes a procedure which selects zero or more items from the database.
   *
   * @param procName Name of the procedure to invoke.
   * @param parameters Parameters to pass to the procedure.
   */
  protected callSelectManyProc<TResult>(procName: string, parameters: any[]) {
    const p = new Promise<TResult[]>((resolve, reject) => {
      connectionPool.getConnection((connectError, conn) => {
        if (connectError) {
          reject(this.createDbError(connectError));
          return;
        }

        this.invokeStoredProc(
          conn,
          procName,
          parameters,
          (error: MysqlError | null, results: any[]) => {
            if (error) {
              debug(
                `callSelectManyProc: Call to ${procName} failed: ${error.message}`
              );
              reject(this.createDbError(error));
            } else {
              try {
                resolve(results[0] as TResult[]);
              } catch (err) {
                debug(
                  `callSelectManyProc: Result processing failed: ${
                    (err as Error).message
                  }`
                );
                reject(error);
              }
            }
          }
        );
        conn.release();
      });
    });

    return p;
  }

  /**
   * Invokes a procedure which selects a single item from the database.
   *
   * @param procName Name of the procedure to invoke.
   * @param parameters Parameters to pass to the procedure.
   */
  protected callSelectOneProc<TResult>(procName: string, parameters: any[]) {
    const p = new Promise<TResult>((resolve, reject) => {
      connectionPool.getConnection((connectError, conn) => {
        if (connectError) {
          reject(this.createDbError(connectError));
          return;
        }

        this.invokeStoredProc(
          conn,
          procName,
          parameters,
          (error: MysqlError | null, results: any[]) => {
            if (error) {
              debug(
                `callSelectOneProc: Call to ${procName} failed: ${error.message}`
              );
              reject(this.createDbError(error));
            } else {
              try {
                const dataResult = results[0] as any[];
                if (dataResult.length === 0) {
                  reject(
                    new DbError(DbErrorCode.ItemNotFound, 'Item not found.')
                  );
                } else {
                  resolve(dataResult[0] as TResult);
                }
              } catch (err) {
                debug(
                  `callSelectOneProc: Result processing failed: ${
                    (err as Error).message
                  }`
                );
                reject(error);
              }
            }
          }
        );
        conn.release();
      });
    });

    return p;
  }

  /**
   * Invokes a procedure which changes data in the database.
   *
   * All DML procedures return two one-row result sets:
   *
   * 1) Operation result including err_code and err_context.
   * 2) The data for the element that was added, updated or deleted.
   *
   * @param procName Name of the stored procedure to execute.
   * @param parameters Parameters to provide to the procedure.
   */
  protected callChangeProc<TResult>(procName: string, parameters: any[]) {
    const p = new Promise<TResult>((resolve, reject) => {
      connectionPool.getConnection((connectError, conn) => {
        if (connectError) {
          reject(this.createDbError(connectError));
          return;
        }

        this.invokeStoredProc(
          conn,
          procName,
          parameters,
          (error: MysqlError | null, results: any[][]) => {
            if (error) {
              debug(
                `callChangeProc: Call to ${procName} failed: ${error.message}`
              );
              reject(this.createDbError(error));
            } else {
              try {
                debug(
                  `callChangeProc: Number of result sets: ${results.length}`
                );

                // The DML operation was successful.  The result
                // set contains information about the item that was inserted,
                // updated or deleted.
                resolve(results[0][0] as TResult);
              } catch (err) {
                debug(
                  `callChangeProc: Result processing failed: ${
                    (err as Error).message
                  }`
                );
                reject(error);
              }
            }
          }
        );
        conn.release();
      });
    });

    return p;
  }

  /**
   * Executes a stored procedure.
   *
   * @param procName Name of the procedure to execute.
   * @param parameters Parameters to pass to the procedure.
   * @param callback Function to call with the results.
   */
  private invokeStoredProc(
    conn: Connection,
    procName: string,
    parameters: any[],
    callback?: queryCallback
  ): Query {
    const placeholders = parameters.length
      ? '?' + ',?'.repeat(parameters.length - 1)
      : '';
    return this.query(
      conn,
      `call ${procName}(${placeholders})`,
      parameters,
      callback
    );
  }

  /**
   * Converts the object returned for MySQL bit fields into a
   * more consumable boolean.
   *
   * https://stackoverflow.com/questions/34414659
   *
   * @param jsonObject JSON object returned from MySQL.
   * @param bitFields Names of the bit fields to convert.
   */
  protected convertBitFields(
    jsonObject: { [key: string]: any },
    bitFields: string[]
  ) {
    const newObject = {
      ...jsonObject
    };

    for (const bitField of bitFields) {
      newObject[bitField] =
        (jsonObject[bitField] as number[]).lastIndexOf(1) !== -1;
    }

    return newObject;
  }

  private createDbError(error: MysqlError) {
    switch (error.sqlState) {
      case SqlStateValue.NotFound:
        return new DbError(
          DbErrorCode.ItemNotFound,
          'Item not found',
          error.sqlMessage
        );

      case SqlStateValue.AccessDenied:
        return new DbError(
          DbErrorCode.NotAuthorized,
          'Access denied.',
          error.sqlMessage
        );

      case SqlStateValue.DuplicateKey:
        return new DbError(
          DbErrorCode.DuplicateItemExists,
          'Duplicate item exists.',
          error.sqlMessage
        );

      case SqlStateValue.UserDefiniedError:
        for (const v of Object.values(DbError)) {
          if (error.sqlMessage === v) {
            return new DbError(
              error.sqlMessage as DbErrorCode,
              'A user defined error occurred.',
              error.sqlMessage
            );
          }
        }

      // Fall through

      default:
        return new DbError(
          DbErrorCode.UnexpectedError,
          `An unexpected error occurred (sqlState: ${
            error.sqlState || 'Unknown'
          }).`,
          `sqlMessage: ${error.sqlMessage || 'Unknown'} sql: ${
            error.sql || 'Unknown'
          }`
        );
    }
  }
}
