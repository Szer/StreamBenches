using System;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Kusto.Data.Common;
using static StreamBenches.Tasks;
using static StreamBenches.Common;

namespace StreamBenches.CSharp
{
    public class Channels
    {
        private record WorkItem(string Cluster, ICslQueryProvider Provider, DateTime Start, DateTime End);
        
        private static async Task ImplInternal(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            perfTask();
            
            var workItemsChannel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(100) { SingleWriter = true });
            _ = Task.Run(async () =>
            {
                foreach (var i in dateQuants.SelectMany(p => kustoClients, (l, r) => (l.Item1, l.Item2, r.Item1, r.Item2)))
                {
                    await workItemsChannel.Writer.WriteAsync(new WorkItem(i.Item3, i.Item4, i.Item1, i.Item2));
                }
            });

            var entriesChannel = Channel.CreateBounded<(string cluster, DateTime start, string[] logs)>(new BoundedChannelOptions(10) { SingleReader = true });
            for (int i = 0; i < parallelism; i++)
            {
                _ = Task.Run(async () =>
                {
                    await foreach (var workItem in workItemsChannel.Reader.ReadAllAsync())
                    {
                        var logs = await logsBetween(workItem.Provider, workItem.Start, workItem.End);
                        await entriesChannel.Writer.WriteAsync((workItem.Cluster, workItem.Start, logs.Select(p => p.ToString()).ToArray()));
                    }
                });
            }

            await foreach (var (cluster, start, logs) in entriesChannel.Reader.ReadAllAsync())
            {
                await afterTask(cluster, start, logs);
            }
        }

        public static void Impl(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            ImplInternal(kustoClients).GetAwaiter().GetResult();
        }
    }
}