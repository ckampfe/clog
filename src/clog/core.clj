(ns clog.core
  (:require [clojure.data.csv :as csv]
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
                                          (datascript.db/-datoms d :eavt []))
                            :schema (datascript.db/-schema d)})))

(def db-read-handler
  (transit/read-handler (fn [r] (datascript.db/db-from-reader r))))

(defn transit-out [file data]
  (with-open [out (io/output-stream file)]
    (let [tw (transit/writer out
                             :json
                             {:handlers {datascript.db.Datom datom-write-handler
                                         datascript.db.DB db-write-handler}})]
      (transit/write tw data))))

(defn transit-in [file]
  (with-open [rdr (io/input-stream file)]
    (let [tr (transit/reader rdr
                             :json
                             {:handlers {"datascript-datom" datom-read-handler
                                         "datascript-db" db-read-handler}})]
      (transit/read tr))))

(def coercions
  {:to-int (fn [i] (Integer/parseInt i))
   :to-long (fn [i] (Long/parseLong i))})

(defn csv-data->maps [csv-data]
  (cons (first csv-data)
        (pmap zipmap
              (->> (first csv-data)
                   (map keyword)
                   repeat)
              (rest csv-data))))

(defn coerce-row-types [rows typemap]
  (let [header (first rows)
        typemap (clojure.edn/read-string typemap)]
    (pmap (fn [row]
            (reduce (fn [acc [k f]]
                      (update acc
                              k
                              #((get coercions f) %)))
                    row
                    typemap))
          (rest rows))))

(defn create-db [rows]
  (let [headers (keys (first rows))
        database (->> headers
                      (reduce (fn [indexes header]
                                (assoc indexes
                                       header
                                       {:db/index true}))
                              {})
                      data/empty-db
                      data/conn-from-db)]

    (doseq [row rows]
      (data/transact! database [row]))

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
          rows-as-maps (csv-data->maps rows)
          type-coerced-rows (coerce-row-types rows-as-maps typemap)
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
  (let [query (clojure.edn/read-string query)]
    (if (index-exists? (str file ".index"))
      (let [database (load-database-from-index file)]
        (pprint/pprint (data/q query @database)))

      (with-open [rdr (io/reader file)]
        (let [rows (csv/read-csv rdr)
              rows-as-maps (csv-data->maps rows)
              type-coerced-rows (coerce-row-types rows-as-maps typemap)
              database (create-db type-coerced-rows)]

          (pprint/pprint (data/q query @database)))))))

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
            {:code 0 :value (write-index (last arguments) options)}

            (and (:query options)
                 (:typemap options))
            {:code 0 :value (query (last arguments) options)}

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
  (time (-main "/Users/xcxk066/code/clog/junk3.csv" "-i" "-t" "{:id :to-int}"))

  ;; with index
  (time (dotimes [_ 1]
          (println
           (-main "/Users/xcxk066/code/clog/junk3.csv"
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

  (with-open [w (io/writer "junk4.csv")]
    (csv/write-csv w (->> (zipmap (range 1 10001) (cycle rows))
                          (map (fn [[k v]] (vec (cons k v))))
                          (sort-by (fn [row] (first row)))
                          vec)))

  (time (dotimes [_ 1] (println (-main "/Users/xcxk066/code/clog/junk4.csv"
                                       "-q"
                                       "[:find (count ?e) ?name :where [?e :kind \"jolly\"] [?e :name ?name]]")))))
