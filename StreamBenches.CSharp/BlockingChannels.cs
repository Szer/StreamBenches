using System;
using System.Linq;
using Kusto.Data.Common;
using static StreamBenches.Tasks;
using static StreamBenches.Common;

namespace StreamBenches.CSharp
{
    public static class BlockingChannels
    {
        private record WorkItem(string Cluster, ICslQueryProvider Provider, DateTime Start, DateTime End);
        
        public static void Impl(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            perfTask();

            dateQuants
                .SelectMany(p => kustoClients, (l, r) => new WorkItem(r.Item1, r.Item2, l.Item1, l.Item2))
                .AsParallel()
                .WithDegreeOfParallelism(parallelism)
                .Select(p =>
                {
                    var logs = logsBetween(p.Provider, p.Start, p.End).GetAwaiter().GetResult();
                    return (p.Cluster, p.Start, logs.Select(p => p.ToString()).ToArray());
                })
                .ForAll(x =>
                {
                    var (cluster, start, logs) = x;
                    afterTask(cluster, start, logs).GetAwaiter().GetResult();
                });
        }
    }
}