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
                           {:datoms (map (fn [^datascript.db.Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d)])
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
        (map zipmap
             (->> (first csv-data)
                  (map keyword)
                  repeat)
             (rest csv-data))))

(defn coerce-row-types [rows typemap]
  (let [header (first rows)
        typemap (clojure.edn/read-string typemap)]
    (map (fn [row]
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

(defn write-index [file typemap]
  (with-open [rdr (io/reader file)]
    (let [rows (csv/read-csv rdr)
          rows-as-maps (csv-data->maps rows)
          type-coerced-rows (coerce-row-types rows-as-maps typemap)
          database (create-db type-coerced-rows)]
      (transit-out (str file ".index")
                   @database))))

#_(defn write-index [file typemap]
    (with-open [rdr (io/reader file)]
      (let [doc (csv/read-csv rdr)
            rows-as-maps (csv-data->maps doc)
            type-coerced-rows (coerce-row-types rows-as-maps typemap)
            database (create-db type-coerced-rows)]

        (spit (str file ".index")
              (pr-str @database)))))

(defn show-header [file]
  (with-open [rdr (io/reader file)]
    (let [doc (csv/read-csv rdr)]
      (println (first doc)))))

#_(defn load-database-from-index [file]
    (->> ".index"
         (str file)
         slurp
         (clojure.edn/read-string {:readers data/data-readers})
         data/conn-from-db))

(defn load-database-from-index [file]
  (let [t (transit-in (str file ".index"))]
    (data/conn-from-db t)
    #_(data/conn-from-db (datascript.db/db-from-reader t))))

(defn query [file query-string typemap]
  (let [query (clojure.edn/read-string query-string)]
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
  ;; An option with a required argument
  [["-i" "--index" "Create an index"]
   ["-t" "--typemap TYPEMAP" "A typemap of transforms"]
   ["-s" "--show-header" "Show header only"]
   ["-q" "--query QUERY" "Run a query"]
   ["-h" "--help"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 summary)
      (:show-header options) (show-header (first arguments))
      (and (:index options)
           (:typemap options)) (write-index (first arguments)
                                            (:typemap options))
      (and (:query options)
           (:typemap options)) (query (first arguments)
                                      (:query options)
                                      (:typemap options))
      :else (println summary)) #_(println options)
    #_(println arguments)
    #_(println errors)
    #_(println summary)))

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
