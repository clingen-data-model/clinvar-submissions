(ns clinvar-submissions.db
  (:require [taoensso.timbre :as log])
  (:import (org.rocksdb RocksDB RocksDBException Options)
           (java.lang String)))

(defn init!
  "Construct and initialize RocksDB client with given path. Will create
  database if doesn't exist already."
  [^String db-path]
  (log/info "init!" db-path)
  (let [opts (-> (Options.) (.setCreateIfMissing true))]
    (RocksDB/open opts db-path)))

(defn put-record
  "Set key to val"
  [^RocksDB rocksdb ^String key ^String val]
  (log/debug "put-record" key val)
  (.put rocksdb (.getBytes key) (.getBytes val)))

(defn delete-record
  "Delete record with key from database"
  [^RocksDB rocksdb ^String key]
  (log/debug "delete-record" key)
  (.delete rocksdb (.getBytes key)))

(defn get-key
  "Return val that has exact match for key"
  [^RocksDB rocksdb key]
  (log/debug "get-key" key)
  (String. (.get rocksdb (.getBytes key))))

(defn get-prefix
  "Return vector of all (key,val) for which key has given prefix"
  [^RocksDB rocksdb ^String key-prefix]
  ; RocksIterator.seek goes to first key that matches (or after) key.
  ; Since RocksDB is key-sorted, can iterate until first key doesn't start with the prefix.
  (log/debug "get-prefix" key-prefix)
  (let [iterator (.newIterator rocksdb)
        ret (transient [])]
    (.seek iterator (.getBytes key-prefix))
    (while (and (.isValid iterator) (.startsWith (String. (.key iterator)) key-prefix))
      (conj! ret [(String. (.key iterator)) (String. (.value iterator))])
      (.next iterator))
    (persistent! ret)
    ))