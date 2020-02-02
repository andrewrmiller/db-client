/**
 * Utility class to change string case from snake_case to camelCase.
 */
export class ChangeCase {
  /**
   * Produce a clone of the provided object where snake_case property
   * names are converted to camelCase.
   *
   * Note that null values are not included in the clone.
   *
   * @param o The object to convert.
   */
  public static toCamelObject(o: { [key: string]: any }) {
    const newObject: { [key: string]: any } = {};
    Object.keys(o).forEach(k => {
      // Null values are excluded from the generated object.
      if (o[k] !== null) {
        const newKey = ChangeCase.toCamelString(k);
        newObject[newKey] = o[k];
      }
    });

    return newObject;
  }

  /**
   * Change the case of a string from snake case to camel case.
   * @param s chang
   */
  public static toCamelString(s: string) {
    return s.replace(/([-_][a-z])/gi, $1 => {
      return $1
        .toUpperCase()
        .replace('-', '')
        .replace('_', '');
    });
  }
}
