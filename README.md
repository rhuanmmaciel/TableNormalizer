
# Table Normalizer

O objetivo da implementação era utilizar uma linguagem com o paradigma funcional para desenvolver funções que permitissem a normalização de tabelas no formato csv. 



## Leitura e escrita dos arquivos

A leitura e a escrita dos arquivos utiliza uma biblioteca que o clojure disponibiliza.

```clojure
(defn read-csv [path]
  (with-open [rdr (io/reader path)]
    (doall (csv/read-csv rdr))))
```

```clojure
(defn write-csv [data filename]
  (with-open [writ (io/writer filename)]
    (csv/write-csv writ data)))
```

## Criação das chaves primárias

A função *create-index* permite adicionar uma coluna que possa ser o identificador da respectiva tabela. Assim, para criar uma coluna como chave primária, ela deve cumprir esses 3 requisitos: 
- Não possuir valores repetidos
- Não possuir valores nulos
- Não possuir valores em branco

Dessa forma, a função *create-index* foi implementada com as seguintes 3 opções de chave primária:

* Números inteiros sequenciais de 1 a n linhas (1, 2, 3, ..., n)
* Números inteiros sequenciais de n linhas a 1 (n, n-1, n-2, ... 1)
* Letras maiúsculas (A, B, C, ..., Z, AA, AB, ...)

```clojure
(defn create-index [table choice index_column_name]
  (let [func (cond
               (= choice enums/sequential-index) (fn [idx ignored] (+ 1 idx))
               (= choice enums/sequential-index-descending) (fn [idx total] (- total idx 1))
               (= choice enums/sequential-letters) (fn [idx ignored] (str (sequential-letters (+ 1 idx))))
               :else (throw (IllegalArgumentException.)))]
    (if (empty? table)
      []
      (let [total (count table)]
        (let [first-row (conj (vec (concat (first table) [index_column_name])))]
          (cons first-row (map-indexed (fn [idx row] (conj row (func idx total))) (rest table))))))))
```

A função *sequential-letters* serve para retornar as letras na sequencia correta quando for selecionada a terceira opção.

```clojure
(defn sequential-letters [n]
  (loop [n n result ""]
    (if (zero? n)
      result
      (let [quot (int (/ (dec n) 26))
            rem (mod (dec n) 26)
            char (char (+ 65 rem))]
        (recur quot (str char result))))))
```
## Agrupamento de colunas com informação repetida

Algumas colunas podem ser redundantes, por isso uma solução é criar outra tabela que possuem essas colunas redundantes e depois fazem a referência para a tabelo original. Para fazer isso, a função *replace-columns-with-foreign-keys* utiliza as funções abaixo dela.

```clojure
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
```

```clojure
(defn insert-at-index [seq element index]
  (concat (take index seq) (list element) (drop index seq)))
```

```clojure
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
```

```clojure
(defn remove-duplicate-dictionaries [dict-list]
  (reduce (fn [acc dict]
            (if (some #(dictionaries-equal? dict %) acc)
              acc
              (conj acc dict)))
          []
          dict-list))
```

```clojure
(defn dictionaries-equal? [dict1 dict2]
  (and (= (count dict1) (count dict2))
       (every? (fn [k] (= (get dict1 k) (get dict2 k))) (keys dict1))))
```

```clojure
(defn remove-unselected-columns [distinct-groups index-columns]
  (map #(select-keys % index-columns) distinct-groups))
```

```clojure
(defn create-column-map [row index-columns]
  (reduce (fn [col-map index]
            (assoc col-map index (get row index)))
          {}
          index-columns))
```

## Linhas repetidas

Para colocar nas formas normais não podem existir linhas que se repetem, para isso a função *remove-duplicate-rows* as removem.

```clojure
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
```

## Colunas multivaloradas

Não pode existir uma coluna que possui valores não atômicos. Para isso existem essas duas funções:
- *split-multivalued-column* -> separa a coluna multivalorada em n colunas  
- *split-multivalued-column-foreign-key* -> cria uma outra tabela que abriga essas informações utilizando como referência a chave primária

```clojure
(defn split-multivalued-column [data-csv index delimiter new-column-names]
  (let [header (first data-csv)
        data (rest data-csv)
        split-data (mapv
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
                         )) data)
        listed-data (cons (flatten (cons (cons (take index header) new-column-names) (take-last (- (count header) index 1) header))) split-data)]
    (map #(vec %) listed-data)
    )
  )
```

```clojure
(defn split-multivalued-column-foreign-key [data-csv foreign-key-index delimiter multi-value-index]
  (let [data (rest data-csv)
        split-data (mapv
                     (fn [row]
                       (let [foreign-key-value (get row foreign-key-index)
                             multi-value-col-value (get row multi-value-index)
                             delimit-pattern (re-pattern (str delimiter))
                             split-values (str/split multi-value-col-value delimit-pattern)]
                         [foreign-key-value split-values]
                         )) data)
        second-table (mapcat
                       (fn [[foreign-key split-values]]
                         (map (fn [value] [foreign-key value]) split-values))
                       split-data)
        first-table (mapv #(vec (concat (take multi-value-index %) (drop (inc multi-value-index) %))) data-csv)
        ]
    [first-table second-table]
    )
  )
```

## Link para o vídeo

[![Vídeo]()](https://youtu.be/Z7-bc1g6r_M)


