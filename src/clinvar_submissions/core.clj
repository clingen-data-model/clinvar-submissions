(ns clinvar-submissions.core
  (:require [clinvar-submissions.db :as db]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
   (:gen-class))

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     (System/getenv "KAFKA_USER") ; TODO throw exception
                 :kafka-password (System/getenv "KAFKA_PASSWORD")
                 :kafka-consumer-topic    "clinvar-raw-test"})

(def topic-metadata
  {:input
   {:topic-name "clinvar-raw"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}
   :output
   {:topic-name "clinvar-scv"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}})

(defn kafka-config
  "Expects, at a minimum, :kafka-user and :kafka-password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "application.id"                        "clinvar-submissions-local"
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})


(defn select-clinical-assertion
  "Return true if the message is a clinical assertion
  otherwise return nil"
  [[key v]]
  (log/trace "in select-clinical-assertion " key)
  (= "clinical_assertion" (get-in (json/parse-string v true) [:content :entity_type])))

(def rocksdb-path "/tmp/clinvar-submissions-rocksdb/")
(defonce rocksdb (db/init! rocksdb-path))

(def key-functions
  {:variation_archive (fn [id] (assert (not (empty? id))) (str "variation_archive-" id))
   :rcv_accession (fn [id] (str "rcv_accession-" id))
   :trait_set (fn [id] (str "trait_set-" id))
   :trait (fn [id] (str "trait-" id))
   :clinical_assertion_observation (fn [id] (str "clinical_assertion_observation-" id))

   })

(defn get-entity-key
  [entity]
  ;(println entity)
  (let [entity-content (:content entity)
        entity-type (:entity_type entity-content)]
    (case entity-type
      "trait_mapping" (str entity-type ; TODO combine all fields as the key, record is immutable
                           "-" (:clinical_assertion_id entity-content)
                           "-" (:trait_type entity-content)
                           "-" (:mapping_type entity-content)
                           "-" (:mapping_value entity-content)
                           "-" (:mapping_ref entity-content)
                           "-" (:medgen_id entity-content)
                           "-" (:mapping_name entity-content))
      "gene_association" (str entity-type ; association table variation<->gene
                              "-" (:variation_id entity-content)
                              "-" (:gene_id entity-content))
      "clinical_assertion" (str entity-type
                                 "-" (:variation_archive_id entity-content)
                                 "-" (:rcv_accession_id entity-content))
      (str entity-type "-" (:id entity-content))
      )))

(def previous-entity-type (atom ""))

(defn to-rocksdb
  ""
  [[key val]]
  ; Put each message in rocksdb
  (let [val-map (json/parse-string val true)
        id (get-entity-key val-map)]
    ; Log entity-type changes for monitoring stream
    (if (not (= @previous-entity-type (-> val-map :content :entity_type)))
      ; TODO change to logging
      (do
        (printf "entity-type changed from %s to %s\n" @previous-entity-type (-> val-map :content :entity_type))
        (reset! previous-entity-type (-> val-map :content :entity_type)))
      )

    (println "storing" id)
    (case (:type val-map)
      "create" (db/put-record rocksdb id val)
      "update" (db/put-record rocksdb id val)
      "delete" (db/delete-record rocksdb id)
      (ex-info "Message type not [create|update|delete]" {}))
    ))

(defn build-clinical-assertion
  ""
  [[key val]]
  ; For the clinical assertion record val, combine all linked entities
  (let [clinical-assertion (json/parse-string val true)
        content (:content clinical-assertion)]
    (println val)
    (println "building clinical assertion" (:id content))
    ; variation archive
    (let [variation-archive-id (:variation_archive_id content)
          variation-archive-key ((:variation_archive key-functions) variation-archive-id)
          variation-archive (db/get-key rocksdb variation-archive-key)

          rcv-key ((:rcv_accession key-functions) (:rcv_accession_id content))
          rcv (db/get-key rocksdb rcv-key)

          clinical-assertion-observation-ids (:clinical-assertion-observation-ids content)
          clinical-assertion-observation-keys (map clinical-assertion-observation-ids
                                                   #((:clinical-assertion-observation key-functions) %))
          clinical-assertion-observations (map clinical-assertion-observation-keys
                                               #(db/get-key rocksdb %))
          ]

      ; return key with string serialized clinical assertion
      [key
       (-> clinical-assertion

            (assoc :variation_archive variation-archive)
            (dissoc :variation_archive_id)

            (assoc :rcv rcv)
            (dissoc :rcv_accession_id)
        )
       ]
      ))
  )


(defn topology [builder in-topic out-topic]
    (-> (j/kstream builder in-topic)
        ;; Parse to map and stash data in RocksDB
        (j/peek to-rocksdb)
        ; Convert value to map
        ;(j/map (fn [[k v]] [k (json/parse-string v true)]))
        ;; Xform clinical assertion using stored data
        (j/filter select-clinical-assertion)
;        (j/filter (fn [[k v]] (= "SCV000715423" (-> v :content :id))))
        (j/map build-clinical-assertion)
        ; Serialize value back to string
        (j/map (fn [[k v]] [k (json/generate-string v)]))
        (j/to out-topic))
    builder)

(defn write-cfg-to-file
  [kafka-config]
  (with-open [writer (io/writer "kafka.properties")]
    (doseq [[k v] kafka-config]
      (.write writer (str k "=" v "\n")))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (write-cfg-to-file (kafka-config app-config))
  (log/set-level! :info)
  (let [builder (topology (j/streams-builder) (:input topic-metadata) (:output topic-metadata))
        app (j/kafka-streams builder (kafka-config app-config))]
    (j/start app)))



