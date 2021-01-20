namespace StreamBenches.CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            BlockingChannels.Impl(Data.Fake.kustoClients);
        }
    }
}
