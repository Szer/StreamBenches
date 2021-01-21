using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Channels;
using System.Threading.Tasks;
using Kusto.Data.Common;
using static StreamBenches.Tasks;
using static StreamBenches.Common;
using System.Reactive.Linq;
using System.Reactive.Disposables;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;

namespace StreamBenches.CSharp
{
    public class TrillPlayground
    {
        private record WorkItem(string Cluster, DateTime Start, DateTime End, GatewayLogEntry Entry);

        private static async Task ImplInternal(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            perfTask();

            var input = GetData(kustoClients).ToObservable();

            var output = input.ToAtemporalStreamable()
                .TumblingWindowLifetime(1000)
                .GroupAggregate(
                    p => p.Cluster,
                    p => new ListAggregate<WorkItem>(),
                    (cluster, data) => new { cluster, data });
            
            await output.ToStreamEventObservable().ForEachAsync(p =>
            {
                if (p.IsData)
                    afterTask(p.Payload.cluster.Key, p.Payload.data[0].Start, p.Payload.data.Select(p => p.ToString()).ToArray()).GetAwaiter().GetResult();
            });
        }

        private static async IAsyncEnumerable<WorkItem> GetData(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            foreach (var i in dateQuants.SelectMany(p => kustoClients, (l, r) => (l.Item1, l.Item2, r.Item1, r.Item2)))
            {
                var logItems = await logsBetween(i.Item4, i.Item1, i.Item2);
                foreach (var logItem in logItems)
                {
                    yield return new WorkItem(i.Item3, i.Item1, i.Item2, logItem);
                }
            }
        }

        public static void Impl(Tuple<string, ICslQueryProvider>[] kustoClients)
        {
            ImplInternal(kustoClients).GetAwaiter().GetResult();
        }

        internal class ListAggregate<T> : IAggregate<T, List<T>, List<T>>
        {
            private static readonly Expression<Func<List<T>>> init = () => new List<T>();
            public Expression<Func<List<T>>> InitialState() => init;

            public Expression<Func<List<T>, long, T, List<T>>> Accumulate()
            {
                Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Add(input);
                var block = Expression.Block(temp.Body, temp.Parameters[0]);
                return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
            }

            public Expression<Func<List<T>, long, T, List<T>>> Deaccumulate()
            {
                Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Remove(input);
                var block = Expression.Block(temp.Body, temp.Parameters[0]);
                return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
            }

            public Expression<Func<List<T>, List<T>, List<T>>> Difference() => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

            private static List<T> SetExcept(List<T> left, List<T> right)
            {
                var newList = new List<T>(left);
                foreach (var t in right) newList.Remove(t);
                return newList;
            }

            public Expression<Func<List<T>, List<T>>> ComputeResult() => l => l;
        }
    }
}
