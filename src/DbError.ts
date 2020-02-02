/**
 * Codes returned from the database when errors occur.
 */
export enum DbErrorCode {
  NoError = 0,
  ItemNotFound = 1,
  DuplicateItemExists = 2,
  QuotaExceeded = 3,
  MaximumSizeExceeded = 4,
  ItemTooLarge = 5,
  ItemIsExpired = 6,
  ItemAlreadyProcessed = 7,
  InvalidFieldValue = 8,
  NotAuthorized = 9,
  UnexpectedError = 999
}

/**
 * Database error object.  Raised when database errors occur.
 */
export class DbError extends Error {
  public errorCode: DbErrorCode;
  public message: string;
  public context?: string;

  constructor(errorCode: DbErrorCode, message: string, context?: string) {
    super(message);

    // tslint:disable-next-line:max-line-length
    // https://github.com/Microsoft/TypeScript-wiki/blob/master/Breaking-Changes.md#extending-built-ins-like-error-array-and-map-may-no-longer-work

    Object.setPrototypeOf(this, DbError.prototype);

    this.errorCode = errorCode;
    this.message = message;
    this.context = context;
  }
}
