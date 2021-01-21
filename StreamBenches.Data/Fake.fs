module StreamBenches.Data.Fake

open System
open System.Data
open System.Threading
open System.Threading.Tasks
open Kusto.Data.Common
open FSharp.Control.Tasks
open StreamBenches.Common

type FakeDataReader() =
    let mutable rowsLeft = 100_000L
    
    interface IDataReader with
        member _.Read() =
            Thread.SpinWait 100
            Interlocked.Decrement &rowsLeft >= 0L
        member this.Close() = failwith "todo"
        member this.Depth = failwith "todo"
        member this.GetSchemaTable() = failwith "todo"
        member this.IsClosed = failwith "todo"
        member this.NextResult() = failwith "todo"
        member this.RecordsAffected = failwith "todo"
        member _.GetDateTime n = rowsLeft <<< n |> DateTime
        member _.GetString n = rowsLeft >>> n |> string
        member this.FieldCount = failwith "todo"
        member this.GetBoolean(i) = failwith "todo"
        member this.GetByte(i) = failwith "todo"
        member this.GetBytes(i, fieldOffset, buffer, bufferoffset, length) = failwith "todo"
        member this.GetChar(i) = failwith "todo"
        member this.GetChars(i, fieldoffset, buffer, bufferoffset, length) = failwith "todo"
        member this.GetData(i) = failwith "todo"
        member this.GetDataTypeName(i) = failwith "todo"
        member this.GetDecimal(i) = failwith "todo"
        member this.GetDouble(i) = failwith "todo"
        member this.GetFieldType(i) = failwith "todo"
        member this.GetFloat(i) = failwith "todo"
        member this.GetGuid(i) = failwith "todo"
        member this.GetInt16(i) = failwith "todo"
        member this.GetInt32(i) = failwith "todo"
        member this.GetInt64(i) = failwith "todo"
        member this.GetName(i) = failwith "todo"
        member this.GetOrdinal(name) = failwith "todo"
        member this.GetValue(i) = failwith "todo"
        member this.GetValues(values) = failwith "todo"
        member this.IsDBNull(i) = failwith "todo"
        member this.Dispose() = ()
        member this.Item
            with get(_: int): obj = failwith "todo"
        member this.Item
            with get(_: string): obj = failwith "todo"

type FakeQueryProvider() =
    interface ICslQueryProvider with
        member _.ExecuteQueryAsync(_, _, _) = task {
            do! Task.Delay 1000
            return upcast FakeDataReader()
        }

        member val DefaultDatabaseName = "fake" with get,set
        member this.Dispose() = ()
        member this.ExecuteQuery(databaseName, query, properties) = failwith "todo"
        member this.ExecuteQuery(query, properties) = failwith "todo"
        member this.ExecuteQuery(query) = failwith "todo"
        member this.ExecuteQueryV2Async(databaseName, query, properties) = failwith "todo"

let private createKustoProvider cluster =
    cluster, FakeQueryProvider() :> ICslQueryProvider

let kustoClients =
    kustoClusters
    |> Array.map createKustoProvider