/**
 * Codes returned from the database when errors occur.
 */
export enum DbErrorCode {
  ItemNotFound = 'ITEM_NOT_FOUND',
  DuplicateItemExists = 'DUPLICATE_ITEM_EXISTS',
  QuotaExceeded = 'QUOTA_EXCEEDED',
  MaximumSizeExceeded = 'MAXIMUM_SIZE_EXCEEDED',
  ItemTooLarge = 'ITEM_TOO_LARGE',
  ItemIsExpired = 'ITEM_IS_EXPIRED',
  ItemAlreadyProcessed = 'ITEM_ALREADY_PROCESSED',
  InvalidFieldValue = 'INVALID_FIELD_VALUE',
  NotAuthorized = 'NOT_AUTHORIZED',
  UnexpectedError = 'UNEXPECTED_ERROR'
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
