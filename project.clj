(defproject clinvar-submissions "0.1.0-SNAPSHOT"
  :description "Aggregate clinvar-raw stream output into fully-constructed ClinVar submitted assertions"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.rocksdb/rocksdbjni "6.8.1"]
                 [fundingcircle/jackdaw "0.7.5"]
                 [org.apache.kafka/kafka-clients "2.3.1"]
                 [org.apache.kafka/kafka-streams "2.3.1"]
                 [org.apache.kafka/kafka-streams-test-utils "2.3.1"]
                 [cheshire "5.10.0"]
                 [com.taoensso/timbre "4.10.0"]]
  :main ^:skip-aot clinvar-submissions.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :uberjar-name "clinvar-submissions.jar"}})
