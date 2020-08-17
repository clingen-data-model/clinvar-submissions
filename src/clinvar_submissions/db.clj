(ns clinvar-submissions.db
  (:import (org.rocksdb RocksDB RocksDBException Options)
           (java.lang String)))

(defn init! [db-path]
  (let [opts (-> (Options.) (.setCreateIfMissing true))]
    (RocksDB/open opts db-path)))

(defn put-record
  [rocksdb key val]
  (.put rocksdb (.getBytes key) (.getBytes val)))

(defn delete-record
  [rocksdb key]
  (.delete rocksdb (.getBytes key)))

(defn get-key
  [rocksdb key]
  (String. (.get rocksdb (.getBytes key))))

(defn get-prefix
  [rocksdb key-prefix]
  ; RocksIterator.seek goes to first key that matches (or after) key.
  ; Since RocksDB is key-sorted, can iterate until first key doesn't start with the prefix.
  (let [iterator (.newIterator rocksdb)
        ret (transient [])]
    (.seek iterator (.getBytes key-prefix))
    (while (and (.isValid iterator) (.startsWith (String. (.key iterator)) key-prefix))
      (conj! ret [(String. (.key iterator)) (String. (.value iterator))])
      (.next iterator))
    (persistent! ret)
    ))