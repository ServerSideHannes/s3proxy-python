// Shapes mirror the JSON returned by s3proxy/dashboard/router.py + collectors.py.
// The frontend consumes these as-is; the backend is the contract.

export interface Card {
  label: string;
  value: string;
  unit?: string;
  detail?: string;
  spark?: number[];
  y_label?: string;
  breakdown?: BreakdownItem[];
}

export interface BreakdownItem {
  label: string;
  value: string;
  weight?: number;
}

export interface ActivityRow {
  time: string;
  time_relative?: string;
  operation: string;
  bucket: string;
  object: string;
  status: string;
  size: string;
  client_ip: string;
  latency: string;
}

export interface BucketRow {
  name: string;
  objects: string;
  size: string;
}

export interface KeyRow {
  id: string;
  type: string;
  status: string;
  created: string;
}

export interface StatusPayload {
  header: { title: string; status: string; uptime: string; cluster_wide: boolean };
  cards: {
    requests: Card;
    data_encrypted: Card;
    errors: Card;
    active_buckets: Card;
    [key: string]: Card;
  };
  activity: ActivityRow[];
  buckets: BucketRow[];
  keys: KeyRow[];
  footer: {
    version: string;
    req_per_s: string | number;
    throughput: string;
    last_error: string;
  };
}

export interface SeriesPayload {
  spark: number[];
  spark_times: number[];
}

export interface LogEntry {
  time: string;
  time_relative?: string;
  operation: string;
  bucket: string;
  object: string;
  status: string;
  status_code: number;
  size: string;
  client_ip: string;
  latency: string;
}

export interface LogsPayload {
  entries: LogEntry[];
  operations: string[];
  total: number;
  count: number;
  offset: number;
  has_more: boolean;
}

export interface BucketFolder {
  name?: string;
  prefix: string;
}

export interface BucketObject {
  key: string;
  name?: string;
  encrypted: boolean | null;
  size_h?: string;
  last_modified?: string;
}

export interface BucketPayload {
  folders: BucketFolder[];
  objects: BucketObject[];
  total_objects: number | null;
  offset: number;
  has_more: boolean;
  is_truncated: boolean;
}

export interface ObjectDetail {
  bucket: string;
  key: string;
  size_h: string;
  content_type?: string;
  etag?: string;
  last_modified?: string;
  encrypted: boolean;
  encryption_source?: string;
  metadata?: Record<string, string>;
}
