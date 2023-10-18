(ns main.core
  (
    :require
    [main.enums :as enums]
    [main.normalizer-functions :as func]))

; cria variáveis globais
(def example1 (func/read-csv "csv-examples/example1.csv"))
(def example2 (func/read-csv "csv-examples/example2.csv"))
(def example3 (func/read-csv "csv-examples/example3.csv"))
(def example4 (func/read-csv "csv-examples/example4.csv"))
(def example5 (func/read-csv "csv-examples/example5.csv"))

(defn get-length [info]
  (if (or (string? info) (number? info))
    (count (str info))
    (if (map? info)
      (count (pr-str info))
      1)))

; Printa o csv
(defn print-csv [table]
  (let [column-widths (apply mapv max (mapv #(mapv get-length %) table))
        separator (apply str (interpose "+" (mapv (fn [width] (apply str (repeat (inc width) \-))) column-widths)))
        format-str (apply str (interpose "|" (mapv #(str "| %-" % "s ") column-widths)))]
    (doseq [row table]
      (println separator)
      (apply printf format-str row)
      (println "|"))
    (println separator)))







; Cria uma chave primária numérica de 1 a n linhas
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-index "idCrescente"))
;  )






; Cria uma chave primária numérica de n linhas a 1
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-index-descending "idDecrescente"))
;  )







; Cria uma chave primária com letras maiúculas
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-letters "idLetras"))
;  )





; Cria uma tabela auxiliar para evitar repetições
;(defn -main
;  []
;  (let [tables (func/replace-columns-with-foreign-keys example1 [2 3])]
;    (print-csv (first tables))
;    (println)
;    (print-csv (second tables)))
;  )






;; Remove linhas duplicadas
;(defn -main
;  []
;  (print-csv (func/remove-duplicate-rows example2))
;  )







;; Cria novas colunas para atributos multivalorados
;(defn -main
;  []
;  (print-csv (func/split-multivalued-column example4 1 "," ["Bairro", "Rua", "CEP"]))
;  )










;; Cria nova tabela para atributos multivalorados
;(defn -main
;  []
;  (let [tables (func/split-multivalued-column-foreign-key example3 2 ";" 1)]
;    (print-csv (first tables))
;    (println)
;    (print-csv (last tables)))
;  )






;; Exclui coluna
;(defn -main
;  []
;  (print-csv (func/remove-columns example5 [3]))
;  )









;; Cria o arquivo csv
;(defn -main
;  []
;  (func/write-csv (func/remove-duplicate-rows example2) "/home/rhuan/Documents/teste.csv")
;  )