module StreamBenches.FSharp.Dataflow

open System
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow
open FSharp.Control.Tasks
open StreamBenches.Common
open StreamBenches.Tasks

let private linkOptions = DataflowLinkOptions (PropagateCompletion = true)

let private getLogsBlock(cluster, kustoClient) =
    TransformBlock<DateTime * DateTime,
                   string * DateTime * string[]>(fun (start, finish) -> task {
    let! logEntries = logsBetween(kustoClient, start, finish)
    let allLines = [| for log in logEntries -> log.ToString() |]
    return (cluster, start, allLines)
}, ExecutionDataflowBlockOptions(MaxDegreeOfParallelism = parallelism))

let private writeToFileBlock = ActionBlock<string*DateTime*string[]>(fun (cluster, start, allLines) -> unitTask {
    do! afterTask(cluster, start, allLines)
}, ExecutionDataflowBlockOptions(MaxDegreeOfParallelism = 1))

let impl kustoClients = 
    perfTask() |> ignore
    let result =
        
        kustoClients
        |> Seq.map(fun (cluster, kustoClient) -> unitTask {
            let logs = getLogsBlock (cluster, kustoClient)
            logs.LinkTo(writeToFileBlock, linkOptions) |> ignore
            for start, finish in dateQuants do
                let! _ = logs.SendAsync((start, finish))
                ()
        })
        |> Task.WhenAll
    result.GetAwaiter().GetResult()

