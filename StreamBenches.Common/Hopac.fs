module StreamBenches.Hopac

open System
open System.IO
open System.Threading
open Hopac
open Hopac.Infixes
open Common

let afterJob (db, start, allLines: string []) =
    Job.fromUnitTask(fun _ -> File.AppendAllLinesAsync("result.txt", allLines)) // IO imitation, write all logs to HDD
    >>= fun () ->
        let now = DateTime.UtcNow
        let lag = now - start - initialOffset
        
        // updating perf counters
        Interlocked.Add(&totalRows, allLines.LongLength) |> ignore
        lagDict.AddOrUpdate(db, (lag.TotalSeconds, now), fun _ x -> x) |> ignore
        
        if lag >= TimeSpan.Zero then
            Job.unit()
        else
            // if we are faster than speed of light and lag is negative, we should sleep
            upcast timeOut (lag.Negate())
            
let perfJob: Job<unit> =
    File.Delete "result.txt"
    Console.Clear()
    job {
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
            let rps = totalRows / sec
            Console.WriteLine $"Total RPS: %10d{rps}; total lines: %15d{totalRows}; time passed: %10d{sec}"
            Console.WriteLine $"Avg lag: %10d{avgLag}"
            for KeyValue(db, (lag, lastUpdate)) in lagDict do
                Console.WriteLine $"Lag in %10s{db} - %5.0f{lag}. Last update - {lastUpdate}"
        do! timeOutMillis 1000
    } 
    |> Job.forever