module StreamBenches.Data.Real

open Kusto.Data.Net.Client
open StreamBenches.Common

let private createKustoProvider cluster =
    cluster, KustoClientFactory.CreateCslQueryProvider $"Data Source=https://aadgw{cluster}.kusto.windows.net;Initial Catalog=NetDefaultDB;AAD Federated Security=True"

let kustoClients =
    kustoClusters
    |> Array.map createKustoProvider