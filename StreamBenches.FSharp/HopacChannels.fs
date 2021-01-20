module StreamBenches.FSharp.HopacChannels

open StreamBenches.Common
open StreamBenches.Hopac
open Hopac
open Hopac.Infixes

let private outputCh = Ch()

let private spawnLogProcessor (cluster, kustoClient) (incomingCh: Ch<_>) =
    job {
        let! start, finish = incomingCh
        let! logEntries = logsBetween(kustoClient, start, finish)
        let allLines = [| for log in logEntries -> log.ToString() |]
        do! Ch.give outputCh (cluster, start, allLines)
    }
    |> Job.foreverServer

let private getLogsFromKusto kustoClient = job {
    let incomingCh = Ch()
    for _ = 1 to parallelism do
        do! spawnLogProcessor kustoClient incomingCh
    
    do! dateQuants
        |> Seq.map (Ch.give incomingCh)
        |> Job.seqIgnore
        |> Job.start
}

let impl kustoClients: unit =
    queue perfJob
    kustoClients
    |> Seq.map getLogsFromKusto
    |> Job.seqIgnore
    >>= fun () ->
        outputCh
        |> Alt.afterJob afterJob
        |> Job.forever
    |> run