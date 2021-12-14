export interface IConnectionConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  connectDatabase: string;
  service?: string;
  cluster?: boolean;
  is_ssl?: boolean;
  debug?: boolean;
  networkTimeout?: number;
  connectionTimeout?: number;
}

export interface IConnectionReady {
  worker: {host: string, port: number};
  connectionId: number;
  varcharEncoding: string;
  onClose: Promise<void>;
  disconnect(): Promise<void>;
  query<T>(sql: string, ...replacements: any[]): Promise<IQueryReady<T>>;
  execute<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeMany<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeCursor<T>(sql: string, ...replacements: any[]): Promise<IQueryFetch<T>>;
  executeInsert(sql: string, ...replacements: any[]): Promise<IQueryPut>;
  executeMany<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  getServerProtocolVersion(): number;
  getClientProtocolVersion(): number;
  getSqreamVersion(): string|undefined;
}

export interface IQueryReady<T> {
  sql: string;
  statementId: number;
  enqueue(): Promise<IQueryExecute<T>>;
}

export interface IQueryExecute<T> {
  worker: {host: string, port: number};
  varcharEncoding: string;
  execute(): Promise<IQueryFetch<T>>;
}

export interface IQueryPut {
  columns: IQueryType[], 
  putRow(row: (string|number|null|bigint|ISqNumeric)[]): Promise<boolean>,
  flush(): Promise<void>,
  close(): Promise<void>
}

export interface IQueryFetch<T> {
  queryTypeNamed: IQueryTypeNamed[];
  close: () => Promise<void>
  fetchIterator: (chunkSize?: number) => AsyncGenerator<T[]>
  fetchAll(rowLimit?: number): Promise<T[]>;
  put(): IQueryPut
}

export interface IQueryType {
  isTrueVarChar: boolean;
  nullable: boolean;
  type: [ DllType, number, number ];
}

export interface IQueryTypeNamed extends IQueryType {
  name: string;
}

export interface IExtractedStrings {
  original: string;
  withPlaceholders: string;
  strings: string[];
  semiColons: number[];
  error: null|Error;
}

export enum DllType {
  Bool = "ftBool",
  Byte = "ftByte",
  UByte = "ftUByte",
  Short = "ftShort",
  UShort = "ftUShort",
  Int = "ftInt",
  UInt = "ftUInt",
  Long = "ftLong",
  ULong = "ftULong",
  Float = "ftFloat",
  Double = "ftDouble",
  Varchar = "ftVarchar",
  DateTime = "ftDateTime",
  Date = "ftDate",
  Blob = "ftBlob",
  Numeric = "ftNumeric",
}

export interface IConnection {
  config: IConnectionConfig;
  connect(sessionParams?: {[param: string]: string|number|null|boolean}): Promise<IConnectionReady>;
  execute<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeCursor<T>(sql: string, ...replacements: any[]): Promise<IQueryFetch<T>>;
}

export interface ISqConnection {
  subscribe(type: string, callback: (data?: any) => void): {unsubscribe(): void};
  once(type: string, timeout?: number): Promise<any>;
  send(data: any): void;
  onError(callback: (err: any) => void): {unsubscribe(): void};
  close(reason?: string): void;
  clear(): void;
  interupt(err: Error): void;
  isAlive(): boolean;
  firstBuffer(timeout?: number): Promise<Buffer>;
  setMaxRows(maxRows: number): void;
  getClientProtocolVersion(): number;
  getServerProtocolVersion(): number|undefined;
}

declare class ISqNumeric {
  bigint: bigint;
  scale: number;
  toString(): string;
  toJSON(): string;
  static from(value: number|string|bigint, scale?: number): ISqNumeric;
}

export default class Connection implements IConnection {
  config: IConnectionConfig;
  connect(sessionParams?: {[param: string]: string|number|boolean|null}): Promise<IConnectionReady>;
  execute<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeCursor<T>(sql: string, ...replacements: any[]): Promise<IQueryFetch<T>>;
  executeInsert(sql: string, ...replacements: any[]): Promise<IQueryPut>;
  executeMany<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  constructor(config: IConnectionConfig);
  static sqConnect(host: string, port?: string, is_ssl?: boolean, debug?: boolean): Promise<ISqConnection>;
  static sqlSanitize(sql: string, replacements?: (string|number|null|boolean|undefined)[]): {words: (string[])[], statements: string[]};
  static extractStrings(string: string): IExtractedStrings;
  static SqNumeric: (new () => ISqNumeric) & {from(value: number|string|bigint|ISqNumeric, scale?: number): ISqNumeric;};
  /**
   * Compares verion strings.
   * 
   * @param v1 
   * @param v2 
   * 
   * @returns
   * - -1 = v1 < v2
   * -  0 = v1 == v2
   * -  1 = v1 > v2
   * - undefined = invalid versions
   */
  static versionCompare(v1: string|undefined, v2: string|undefined): -1|0|1|undefined;
}

declare module '@sqream/sqreamdb' {
  export = Connection;
}
