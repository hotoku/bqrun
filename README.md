# bqrun

## 概要
ディレクトリの中にあるsqlファイルを全て読み、依存関係を解析し（あるファイルAで`select .. from`されているテーブルが別のファイルBで`create table`されていた場合、
BはAより前に実行される）、順番に実行するためのMakefileを作成、makeを実行する。

## 外部依存
1. docker
2. graphviz（dotコマンド）

## インストール
1. `pip install bqrun`

## 前提
1. 1つのディレクトリの中に全てのSQLファイルが入っていること
1. 全てのSQLファイルは拡張子`.sql`を持つこと、かつ、クエリ以外に`.sql`で終わるファイルがないこと
## 大まかな動作
1. 全SQLファイルを読んで依存関係を解析、依存関係に従ったMakefileを作成する
1. このMakefileにより、各ファイルに対し以下のようなコマンドが実行される
    1. 各SQLファイルを`bq query`に投げる
    1. `done.<base name>` というファイルを作成する(`<base name>`は、ファイル名の拡張子以外)
1. 2回目以降の実行では、各ファイルについて`done.<base name>`ファイルのタイムスタンプと、依存先のファイルのタイムスタンプを比較し再実行が必要な部分だけが実行される

## オプション
1. `-p=<num>`または`--parallel=<num>`: 並列実行数を指定（デフォルトは8）
1. `-b`または`--binary`: SQLの依存性解析に利用する[`alphasql`](https://github.com/Matts966/alphasql)を、Dockerを経由せずに利用する。別途`alphadag`のインストールが必要。インストール方法は、`alphasql`のreadmeを参照。
