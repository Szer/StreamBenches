module StreamBenches.Common

open System
open System.Collections.Concurrent
open System.Text
open Kusto.Data.Common
open FSharp.Control.Tasks

let kustoClusters = [|
    "ams"
    "chi"
    "dms" 
    "dub" 
    "est"
    "hkg"
    "kwa" 
    "mel"
    "san"
    "sin"
    "syd"
    "wst"
|]

let toUtcStr (date: DateTime) =
    date.ToString "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"

type GatewayLogEntry =
    { EnvTime: DateTime
      EnvCloudLocation: string
      GatewayRequestId: string
      StatusCode: string
      TargetService: string }
    override this.ToString() =
        let sb = StringBuilder()
        sb.Append(toUtcStr this.EnvTime) |> ignore
        sb.Append(',')                   |> ignore
        sb.Append(this.EnvCloudLocation) |> ignore
        sb.Append(',')                   |> ignore
        sb.Append(this.GatewayRequestId) |> ignore
        sb.Append(',')                   |> ignore
        sb.Append(this.StatusCode)       |> ignore
        sb.Append(',')                   |> ignore
        sb.Append(this.TargetService)|> ignore
        sb.ToString()

let logsBetween (kustoClient: ICslQueryProvider, start, finish) = task {
    let query = $"""RequestSummaryEventCore
| where env_time >= datetime({toUtcStr start})
| where env_time < datetime({toUtcStr finish})
| project env_time, env_cloud_location, GatewayRequestId, StatusCode, TargetService"""
    let! reader = kustoClient.ExecuteQueryAsync("AADGatewayProd", query, null)
    return seq {
        use reader = reader
        while reader.Read() do
            yield { EnvTime = reader.GetDateTime 0
                    EnvCloudLocation = reader.GetString 1
                    GatewayRequestId = reader.GetString 2
                    StatusCode = reader.GetString 3
                    TargetService = reader.GetString 4 }
    }
}

let offset = TimeSpan.FromMilliseconds 100.
let initialOffset = TimeSpan.FromMinutes 15.
let parallelism = Environment.ProcessorCount * 2

let perfDate = DateTime.UtcNow
let mutable totalRows = 0L
let lagDict = ConcurrentDictionary<string, double * DateTime>()

let dateQuants =
    seq {
        let mutable start = DateTime.UtcNow.Subtract initialOffset // Start from the past, because logs are lagging
        while true do
            let finish = start.Add offset
            yield (start, finish)
            start <- finish
    }
