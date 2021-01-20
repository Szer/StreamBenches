module StreamBenches.Hopac

open System
open System.Collections.Generic
open System.IO
open Hopac
open Hopac.Infixes
open Common

let perfCounter = MVar ((0L, Dictionary<string, (float * DateTime)>()))

let afterJob (db, start, allLines: string []) =
    Job.fromUnitTask(fun _ -> File.AppendAllLinesAsync("result.txt", allLines)) // IO imitation, write all logs to HDD
    >>= fun () ->
        let now = DateTime.UtcNow
        let lag = now - start - initialOffset
        
        // updating perf counters
        MVar.mutateFun(fun (counter, (lagDict: Dictionary<string, float * DateTime>)) ->
            lagDict.[db] <- (lag.TotalSeconds, now)
            counter + allLines.LongLength, lagDict) perfCounter
        |> queue
        
        if lag >= TimeSpan.Zero then
            Job.unit()
        else
            // if we are faster than speed of light and lag is negative, we should sleep
            upcast timeOut (lag.Negate())
            
let perfJob: Job<unit> =
    Console.Clear()
    job {
        let! counter, lagDict = MVar.read perfCounter

        Console.SetCursorPosition(0, 0);
        let now = DateTime.UtcNow
        let timeDiff = now - perfDate
        let sec = int64 timeDiff.TotalSeconds
        
        let avgLag =
            if lagDict.Count = 0 then 0 else
            lagDict
            |> Seq.averageBy (fun (KeyValue(key, v)) -> fst v)
            |> int
        
        if sec > 0L then
            let rps = counter / sec
            Console.WriteLine $"Total RPS: %10d{rps}; total lines: %15d{counter}; time passed: %10d{sec}"
            Console.WriteLine $"Avg lag: %10d{avgLag}"
            for KeyValue(db, (lag, lastUpdate)) in lagDict do
                Console.WriteLine $"Lag in %10s{db} - %5.0f{lag}. Last update - {lastUpdate}"
        do! timeOutMillis 1000
    } 
    |> Job.forever