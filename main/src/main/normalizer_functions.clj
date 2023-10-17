(ns main.normalizer-functions
  (
    :require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [main.enums :as enums]))

(defn read-csv [path]
  (with-open [rdr (io/reader path)]
    (doall (csv/read-csv rdr))))

(defn write-csv [data filename]
  (with-open [writ (io/writer filename)]
    (csv/write-csv writ data)))

(defn sequential-letters [n]
  (loop [n n result ""]
    (if (zero? n)
      result
      (let [quot (int (/ (dec n) 26))
            rem (mod (dec n) 26)
            char (char (+ 65 rem))]
        (recur quot (str char result))))))

(defn create-index [table choice index_column_name]
  (let [func (cond
               (= choice enums/sequential-index) (fn [idx ignored] (+ 1 idx))
               (= choice enums/sequential-index-descending) (fn [idx total] (- total idx 1))
               (= choice enums/sequential-letters) (fn [idx ignored] (str (sequential-letters (+ 1 idx))))
               :else (throw (IllegalArgumentException.)))]
    (if (empty? table)
      []
      (let [total (count table)]
        (let [first-row (conj (vec (concat (rest (first table)) [index_column_name])))]
          (cons first-row (map-indexed (fn [idx row] (conj row (func idx total))) (rest table))))))))


(defn create-column-map [row index-columns]
  (reduce (fn [col-map index]
            (assoc col-map index (get row index)))
          {}
          index-columns))

(defn remove-unselected-columns [distinct-groups index-columns]
  (map #(select-keys % index-columns) distinct-groups))

(defn dictionaries-equal? [dict1 dict2]
  (and (= (count dict1) (count dict2))
       (every? (fn [k] (= (get dict1 k) (get dict2 k))) (keys dict1))))

(defn remove-duplicate-dictionaries [dict-list]
  (reduce (fn [acc dict]
            (if (some #(dictionaries-equal? dict %) acc)
              acc
              (conj acc dict)))
          []
          dict-list))

(defn remove-columns [updated-data indexes]
  (mapv (fn [row]
          (reduce-kv (fn [acc idx val]
                       (if (not (contains? (set (map int indexes)) (int idx)))
                         (conj acc val)
                         acc)
                       )
                     []
                     row))
        updated-data))

(defn insert-at-index [seq element index]
  (concat (take index seq) (list element) (drop index seq)))

(defn replace-columns-with-foreign-keys [data-csv index-columns]
  (let [data-without-header (rest data-csv)
        data-header (first data-csv)
        column-indices (map int index-columns)
        distinct-groups (map #(create-column-map % column-indices) data-without-header)
        remaining-columns (map #(get data-header %) index-columns)
        common-columns (flatten (remove #(contains? (set remaining-columns) %) data-header))
        common-columns (insert-at-index common-columns "foreignKey" (first index-columns))
        filtered-groups (remove-duplicate-dictionaries (remove-unselected-columns distinct-groups index-columns))
        foreign-keys (zipmap filtered-groups (range (count filtered-groups)))
        updated-data (mapv (fn [row]
                             (let [group (create-column-map row column-indices)
                                   index (get foreign-keys group)]
                               (update row (first index-columns) (constantly index))))
                           data-without-header)
        updated-data (remove-columns updated-data (rest index-columns))
        new-header (flatten (cons (map #(get data-header %) index-columns) ["Index"]))]
    [(cons (vec common-columns) updated-data) (cons new-header (map flatten (mapv (fn [k]
                                                                                    [(vals k) (get foreign-keys (select-keys (create-column-map k column-indices) index-columns))])
                                                                                  filtered-groups)))]))

(defn remove-duplicate-rows [data-csv]
  (let [header (first data-csv)
        data (rest data-csv)
        seen-rows (atom #{})
        unique-data (atom [])]

    (doseq [row data]
      (if (not (contains? @seen-rows row))
        (do
          (swap! seen-rows conj row)
          (swap! unique-data conj row))))

    (cons header @unique-data)))

(defn split-multivalued-column [data-csv index delimiter new-column-names]
  (let [split-data (mapv
                     (fn [row]
                       (let [original-col-value (get row index)
                             delimit-pattern (re-pattern (str delimiter))
                             split-values (str/split original-col-value delimit-pattern)
                             diff-count (- (count new-column-names) (count split-values))
                             filled-diff (if (<= diff-count 0)
                                           []
                                           (repeat diff-count nil))]
                         (concat (take index row)
                                 split-values
                                 filled-diff
                                 (drop (inc index) row))
                         )) (rest data-csv))
        ]
    (cons (first data-csv) split-data)
    )
  )






(defn group-columns-by-master [data-csv master-columns group-columns]
  (let [data-header (first data-csv)
        data-without-header (rest data-csv)
        master-indices (map data-header master-columns)
        group-indices (map data-header group-columns)
        common-indices (into (set master-indices) group-indices)
        distinct-groups (map #(select-keys % common-indices) data-without-header)
        foreign-keys (zipmap distinct-groups (range (count distinct-groups)))
        updated-data (mapv (fn [row]
                             (let [group (select-keys row common-indices)
                                   index (get foreign-keys group)]
                               (assoc row (first master-indices) index)))
                           data-without-header)
        new-header (concat (map #(get data-header %) master-columns) ["Index"])]
    [(vec new-header) updated-data]))
