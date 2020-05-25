import createDebug from 'debug';
import mysql, { FieldInfo, MysqlError, Query, queryCallback } from 'mysql';
import { DbError, DbErrorCode } from './DbError';

const debug = createDebug('westlakelabs:database');

/**
 * Configuration information for the database.
 */
export interface IDatabaseConfig {
  host: string;
  user: string;
  password: string;
  name: string;
}

/**
 * Structure of the first result set returned from all stored procedures.
 * Provides information on the success or failure of the operation.
 */
interface IDbResult {
  err_code: DbErrorCode;
  err_context: string;
}

/**
 * Class which exposes common database operations with promise-based results.
 */
export class MySqlDatabase {
  private config: IDatabaseConfig;
  private conn?: mysql.Connection;

  constructor(config: IDatabaseConfig) {
    this.config = config;
  }

  /**
   * Connects to the database using provided configuration information.
   */
  public connect() {
    debug(
      `Connecting to MySQL database ${this.config.name} on host ${this.config.host}.`
    );
    this.conn = mysql.createConnection({
      host: this.config.host,
      user: this.config.user,
      password: this.config.password,
      database: this.config.name,
      // We store dates using the DATETIME type which has no
      // timezone information in MySQL.  The dates are provided
      // to MySQL in UTC.  When we get them back from the database
      // we don't want any timezone translation to occur so we
      // configure the mysql client with timezone='Z'.
      timezone: 'Z'
    });

    this.conn.connect();
  }

  /**
   * Disconnects from the database.
   */
  public disconnect() {
    this.conn!.end();
  }

  /**
   * Executes a database query and returns the results.
   *
   * @param query Database query to execute.
   * @param parameters Parameter values used by the query.
   */
  protected query<TResult>(query: string, parameters?: any[]) {
    this.connect();

    const p = new Promise<TResult>((resolve, reject) => {
      this.conn!.query(query, parameters, (error, results, fields) => {
        if (error) {
          debug(`query: Failed to execute query: ${error.message}`);
          reject(error);
        } else {
          resolve(results as TResult);
        }
      });
    });

    this.disconnect();
    return p;
  }

  /**
   * Invokes a procedure which selects zero or more items from the database.
   *
   * @param procName Name of the procedure to invoke.
   * @param parameters Parameters to pass to the procedure.
   */
  protected callSelectManyProc<TResult>(procName: string, parameters: any[]) {
    this.connect();

    const p = new Promise<TResult[]>((resolve, reject) => {
      this.invokeStoredProc(
        procName,
        parameters,
        (error: MysqlError | null, results: any, fields?: FieldInfo[]) => {
          if (error) {
            debug(
              `callSelectManyProc: Call to ${procName} failed: ${error.message}`
            );
            reject(error);
          } else {
            try {
              debug('Number of result sets:' + results.length);

              // The first one-row result set contains success/failure
              // information.  If the select operation failed (e.g. due
              // to insufficient permissions) then the promise is rejected.
              const result = results[0][0] as IDbResult;
              if (result.err_code !== DbErrorCode.NoError) {
                debug(
                  `callSelectManyProc: Call to ${procName} failed with err_code: ${result.err_code}`
                );
                debug(`and err_context: ${result.err_context}`);
                reject(this.createDbError(result));
              }

              // The second result set contains the selected items.
              resolve(results[1] as TResult[]);
            } catch (error) {
              debug(
                `callSelectManyProc: Result processing failed: ${error.message}`
              );
              reject(error);
            }
          }
        }
      );
    });

    this.disconnect();
    return p;
  }

  /**
   * Invokes a procedure which selects a single item from the database.
   *
   * @param procName Name of the procedure to invoke.
   * @param parameters Parameters to pass to the procedure.
   */
  protected callSelectOneProc<TResult>(procName: string, parameters: any[]) {
    this.connect();

    const p = new Promise<TResult>((resolve, reject) => {
      this.invokeStoredProc(
        procName,
        parameters,
        (error: MysqlError | null, results: any, fields?: FieldInfo[]) => {
          if (error) {
            debug(
              `callSelectOneProc: Call to ${procName} failed: ${error.message}`
            );
            reject(error);
          } else {
            try {
              debug('Number of result sets:' + results.length);

              // The first one-row result set contains success/failure
              // information.  If the select operation failed (e.g. due
              // to insufficient permissions) then the promise is rejected.
              const result = results[0][0] as IDbResult;
              if (result.err_code !== DbErrorCode.NoError) {
                debug(
                  `callSelectOneProc: Call to ${procName} failed with err_code: ${result.err_code}`
                );
                debug(`and err_context: ${result.err_context}`);
                reject(this.createDbError(result));
              }

              // The second result set contains the selected item.
              const dataResult = results[1];
              if (dataResult.length === 0) {
                reject(
                  new DbError(DbErrorCode.ItemNotFound, 'Item not found.')
                );
              } else {
                resolve(dataResult[0] as TResult);
              }
            } catch (error) {
              debug(
                `callSelectOneProc: Result processing failed: ${error.message}`
              );
              reject(error);
            }
          }
        }
      );
    });

    this.disconnect();
    return p;
  }

  /**
   * Invokes a procedure which changes data in the database.
   *
   * All DML procedures return two one-row result sets:
   *     1) Operation result including err_code and err_context.
   *     2) The data for the element that was added, updated or deleted.
   *
   * @param procName Name of the stored procedure to execute.
   * @param parameters Parameters to provide to the procedure.
   */
  protected callChangeProc<TResult>(procName: string, parameters: any[]) {
    this.connect();

    const p = new Promise<TResult>((resolve, reject) => {
      this.invokeStoredProc(procName, parameters, (error, results, fields) => {
        if (error) {
          debug(`callChangeProc: Call to ${procName} failed: ${error.message}`);
          reject(error);
        } else {
          try {
            debug('Number of result sets:' + results.length);

            // The first one-row result set contains success/failure
            // information.  If the DML operation failed then the
            // promise is rejected.
            const result = results[0][0] as IDbResult;
            if (result.err_code !== DbErrorCode.NoError) {
              debug(
                `callChangeProc: Call to ${procName} failed with err_code: ${result.err_code}`
              );
              debug(`and err_context: ${result.err_context}`);
              reject(this.createDbError(result));
            }

            // The DML operation was successful.  The second one-row result
            // set contains information about the item that was inserted,
            // updated or deleted.
            resolve(results[1][0] as TResult);
          } catch (error) {
            debug(`callChangeProc: Result processing failed: ${error.message}`);
            reject(error);
          }
        }
      });
    });

    this.disconnect();
    return p;
  }

  /**
   * Executes a stored procedure.
   *
   * @param procName Name of the procedure to execute.
   * @param parameters Parameters to pass to the procedure.
   * @param callback Function to call with the results.
   */
  protected invokeStoredProc(
    procName: string,
    parameters: any[],
    callback?: queryCallback
  ): Query {
    const placeholders = parameters.length
      ? '?' + ',?'.repeat(parameters.length - 1)
      : '';
    return this.conn!.query(
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
      newObject[bitField] = (jsonObject[bitField].lastIndexOf(1) !==
        -1) as boolean;
    }

    return newObject;
  }

  /**
   * Creates a new DbError from a DML response.
   *
   * @param response The DML response to convert.
   */
  private createDbError(response: IDbResult) {
    let errorMessage: string;
    switch (response.err_code) {
      case DbErrorCode.ItemNotFound:
        errorMessage = 'Item not found.';
        break;

      case DbErrorCode.DuplicateItemExists:
        errorMessage = 'Duplicate item already exists.';
        break;

      case DbErrorCode.QuotaExceeded:
        errorMessage = 'Quote has been exceeded.';
        break;

      case DbErrorCode.MaximumSizeExceeded:
        errorMessage = 'The maximum size has been exceeded.';
        break;

      case DbErrorCode.ItemTooLarge:
        errorMessage = 'Item is too large.';
        break;

      case DbErrorCode.ItemIsExpired:
        errorMessage = 'Item is expired.';
        break;

      case DbErrorCode.ItemAlreadyProcessed:
        errorMessage = 'Item has already been processed.';
        break;

      case DbErrorCode.InvalidFieldValue:
        errorMessage = 'Invalid field value.';
        break;

      case DbErrorCode.NotAuthorized:
        errorMessage = 'Not authorized';
        break;

      case DbErrorCode.UnexpectedError:
      default:
        errorMessage = `An unexpected error occurred (Error Code: ${response.err_code}).`;
        break;
    }

    return new DbError(response.err_code, errorMessage, response.err_context);
  }
}
