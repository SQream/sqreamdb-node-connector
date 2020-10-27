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
}

export interface IConnectionReady {
  worker: {host: string, port: number};
  connectionId: number;
  varcharEncoding: string;
  onClose: Promise<void>;
  disconnect(): Promise<void>;
  query<T>(sql: string, ...replacements: any[]): Promise<IQueryReady<T>>;
  execute<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeCursor<T>(sql: string, ...replacements: any[]): Promise<IQueryFetch<T>>;
  getServerProtocolVersion(): number;
  getClientProtocolVersion(): number;
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

export interface IQueryFetch<T> {
  queryTypeNamed: IQueryTypeNamed[];
  close: () => Promise<void>
  fetchIterator: (chunkSize?: number) => AsyncGenerator<T[]>
  fetchAll(rowLimit?: number): Promise<T[]>;
}

export interface IQueryTypeNamed {
  isTrueVarChar: boolean;
  name: string;
  nullable: boolean;
  type: [ DllType, number, number ];
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

export default class Connection implements IConnection {
  config: IConnectionConfig;
  connect(sessionParams?: {[param: string]: string|number|boolean|null}): Promise<IConnectionReady>;
  execute<T>(sql: string, ...replacements: any[]): Promise<T[]>;
  executeCursor<T>(sql: string, ...replacements: any[]): Promise<IQueryFetch<T>>;
  constructor(config: IConnectionConfig);
  static sqConnect(host: string, port?: string, is_ssl?: boolean, debug?: boolean): Promise<ISqConnection>;
  static sqlSanitize(sql: string, replacements?: (string|number|null|boolean|undefined)[]): {words: (string[])[], statements: string[]};
  static extractStrings(string: string): IExtractedStrings;
}

declare module '@sqream/sqreamdb' {
  export = Connection;
}
