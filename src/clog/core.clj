(ns clog.core
  (:require [clojure.data.csv :as csv]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.tools.cli :as cli]
            [cognitect.transit :as transit]
            [datascript.core :as data])
  (:gen-class))

(set! *warn-on-reflection* true)

(def datom-read-handler
  (transit/read-handler (fn [r] (datascript.db/datom-from-reader r))))

(def datom-write-handler
  (transit/write-handler "datascript-datom"
                         (fn [^datascript.db.Datom d]
                           [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)])))

(def db-write-handler
  (transit/write-handler "datascript-db"
                         (fn [d]
                           {:datoms (pmap (fn [^datascript.db.Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d)])
                                          ;; this should be configurable, but aevt benches fastes on
                                          ;; common aggregates I've tested, so that's default for now
                                          (datascript.db/-datoms d :aevt [])
                                          #_(datascript.db/-datoms d :eavt [])
                                          #_(datascript.db/-datoms d :avet []))
                            :schema (datascript.db/-schema d)})))

(def db-read-handler
  (transit/read-handler (fn [r] (datascript.db/db-from-reader r))))

(defn transit-out [file data]
  (with-open [out (java.util.zip.DeflaterOutputStream. (io/output-stream file))]
    (let [tw (transit/writer out
                             :json
                             {:handlers {datascript.db.Datom datom-write-handler
                                         datascript.db.DB db-write-handler}})]
      (transit/write tw data))))

(defn transit-in [file]
  (with-open [rdr (java.util.zip.InflaterInputStream. (io/input-stream file))]

    (let [tr (transit/reader rdr
                             :json
                             {:handlers {"datascript-datom" datom-read-handler
                                         "datascript-db" db-read-handler}})]
      (transit/read tr))))

(def coercions
  {:to-int (fn [i] (Integer/parseInt i))
   :to-long (fn [i] (Long/parseLong i))})

(defn row->typed-row [header row]
  (zipmap header row))

(defn coerce-row-types [typemap row]
  (reduce (fn [acc [k f]]
            (update acc
                    k
                    #((get coercions f) %)))
          row
          typemap))

(defn rows-maps-typemapped [rows typemap]
  (let [header (first rows)
        header-keywords (map keyword header)]
    (cons header-keywords (r/foldcat (->> (rest rows)
                                          (r/map (partial row->typed-row header-keywords))
                                          (r/map (partial coerce-row-types typemap)))))))

(defn create-db [rows]
  (let [headers (first rows)
        database (->> headers
                      (reduce (fn [indexes header]
                                (assoc indexes
                                       header
                                       {:db/index true}))
                              {})
                      data/empty-db
                      data/conn-from-db)]

    (dorun (pmap (comp
                  (partial data/transact! database)
                  vec)
                 (partition-all 5000 (rest rows))))

    database))

(defn index-exists? [file]
  (.exists (io/as-file file)))

(defn write-index [file {:keys [typemap csv-separator csv-quote]
                         :or {typemap {}
                              csv-separator \,
                              csv-quote \"}}]
  (with-open [rdr (io/reader file)]
    (let [rows (csv/read-csv rdr
                             :separator csv-separator
                             :quote csv-quote)
          type-coerced-rows (rows-maps-typemapped rows typemap)

          database (create-db type-coerced-rows)]
      (transit-out (str file ".index")
                   @database))))

(defn show-header [file]
  (with-open [rdr (io/reader file)]
    (let [doc (csv/read-csv rdr)]
      (println (first doc)))))

(defn load-database-from-index [file]
  (let [index-db (transit-in (str file ".index"))]
    (data/conn-from-db index-db)))

(defn query [file {:keys [query typemap]
                   :or {typemap {}}}]
  (if (index-exists? (str file ".index"))
    (let [database (load-database-from-index file)]
      (pprint/pprint (data/q query @database)))

    (with-open [rdr (io/reader file)]
      (let [rows (csv/read-csv rdr)
            type-coerced-rows (coerce-row-types rows typemap)
            database (create-db type-coerced-rows)]

        (pprint/pprint (data/q query @database))))))

(def cli-options
  [["-q" "--query QUERY"                 "The query to run"]
   ["-i" "--index"                       "Create an index"]
   ["-t" "--typemap TYPEMAP"             "A typemap of transforms"]
   ["-s" "--csv-separator CSV-SEPARATOR" "CSV separator character" :default \,]
   ["-u" "--csv-quote CSV-QUOTE"         "CSV quote character" :default \"]
   ["-o" "--csv-header"                  "Show CSV header only"]
   ["-v" "--verbose"                     "Verbose mode"]
   ["-h" "--help"]])

(defn exit [status msg]
  (when msg (println msg))
  (System/exit status))

(defn usage [options-summary]
  (->> ["clog:"
        ""
        "Usage: clog [options] file"
        ""
        "Options:"
        ""
        options-summary]
       (clojure.string/join \newline)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (when (:verbose options)
      (println options)
      (println errors))

    (let [return
          (cond
            (:help options)
            {:code 0 :value (usage summary)}

            (= nil (last arguments))
            {:code 1 :value (usage summary)}

            (:csv-header options)
            {:code 0 :value (show-header (last arguments))}

            (and (:index options)
                 (:typemap options))
            {:code 0 :value (write-index (last arguments)
                                         (update options :typemap clojure.edn/read-string))} 

            (and (:query options)
                 (:typemap options))
            {:code 0 :value (query (last arguments)
                                   (-> options (update :query clojure.edn/read-string)
                                       (update :typemap clojure.edn/read-string)))}

            :else {:code 1 :value (usage summary)})]

      (shutdown-agents)
      (exit (:code return)
            (:value return)))))

(comment

  (def rows
    [["bill" "kind"]
     ["sally" "sad"]
     ["ernie" "eerie"]
     ["jill" "jolly"]])

  ;; raw
  (time (dotimes [_ 1]
          (println
           (-main "/Users/xcxk066/code/clog/junk.csv"
                  "-q"
                  "[:find (count ?e) ?name :where [?e :kind \"jolly\"] [?e :name ?name]]"))))

  ;; create index
  (time (-main "-i" "-t" "{:id :to-int}" "/Users/xcxk066/code/clog/junk.csv"))

  ;; with index
  (time (dotimes [_ 15]
          (println
           (-main "/Users/xcxk066/code/clog/junk.csv"
                  "-q"
                  "[:find (count ?e) ?name :where [?e :kind \"jolly\"] [?e :name ?name]]"
                  "-t"
                  "{:id :to-int}"))))

  (-main "/Users/xcxk066/code/clog/example.csv"
         "-q"
         "[:find ?name (count ?e) :where [?e :name ?name]]"
         "-t"
         "{:id :to-int}")

  (-main "/Users/xcxk066/code/clog/example.csv"
         "-q"
         "[:find ?id1 ?id2 ?eq-int :where [?e :id ?id1] [?e2 :id ?id2] [(= ?id1 ?id2) ?eq-int] ]")

  (with-open [w (io/writer "junk6.csv")]
    (csv/write-csv w (->> (zipmap (range 1 200001) (cycle rows))
                          (map (fn [[k v]] (vec (cons k v))))
                          (sort-by (fn [row] (first row)))
                          vec)))

  (time (dotimes [_ 1] (println (-main "/Users/xcxk066/code/clog/junk4.csv"
                                       "-q"
                                       "[:find (count ?e) ?name :where [?e :kind \"jolly\"] [?e :name ?name]]")))))
