module StreamBenches.Tasks

open System
open System.Collections.Concurrent
open System.IO
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks
open Common

let afterTask (db, start, allLines: string []) = task {
    do! File.AppendAllLinesAsync("result.txt", allLines)
    let now = DateTime.UtcNow
    let lag = now - start - initialOffset
    
    // updating perf counters
    Interlocked.Add(&totalRows, allLines.LongLength) |> ignore
    lagDict.AddOrUpdate(db, (lag.TotalSeconds, now), fun _ x -> x) |> ignore

    if lag < TimeSpan.Zero then
        // if we are faster than speed of light and lag is negative, we should sleep
        do! Task.Delay(lag.Negate())
}

let perfTask() = unitTask {
    File.Delete "result.txt"
    Console.Clear()
    while true do
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
    
        do! Task.Delay 1000
}