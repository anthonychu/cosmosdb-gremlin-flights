using CsvHelper;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using NConfig;
using Newtonsoft.Json;
using Polly;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Device.Location;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CosmosDBGremlinFlights.Console
{
    class Program
    {
        public static object Code { get; private set; }

        static void Main(string[] args)
        {
            NConfigurator.UsingFile(@"Secrets.config").SetAsSystemDefault();

            using (DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["CosmosDBUri"]),
                ConfigurationManager.AppSettings["CosmosDBKey"],
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                var graph = client.ReadDocumentCollectionAsync(ConfigurationManager.AppSettings["CosmosDBCollectionLink"]).Result;

                LoadAirports(client, graph).Wait();
            }
        }

        private static async Task LoadAirports(DocumentClient client, DocumentCollection graph)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .RetryAsync(5);

            var airports = new Dictionary<string, Airport>();
            var routes = new HashSet<Tuple<string, string>>();
            using (var httpClient = new HttpClient())
            {
                var airportsCsvStream = await httpClient.GetStreamAsync("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat");
                using (var fileStream = new FileStream("airports.dat", FileMode.Create, FileAccess.Write))
                {
                    await airportsCsvStream.CopyToAsync(fileStream);
                    airportsCsvStream.Close();
                }

                using (var fileStream = new FileStream("airports.dat", FileMode.Open))
                using (var reader = new CsvReader(new StreamReader(fileStream)))
                {
                    var count = 0;
                    while (reader.Read())
                    {
                        var city = Regex.Replace(reader.GetField(2), @"[^\w']", "").Replace("'", @"\'");
                        var country = Regex.Replace(reader.GetField(3), @"[^\w']", "").Replace("'", @"\'");
                        var code = reader.GetField(4);
                        var lat = reader.GetField(6);
                        var lng = reader.GetField(7);

                        if (!new string[] { @"\N", "N/A", "" }.Contains(code))
                        {
                            var gremlinQuery = $"g.addV('airport').property('id', \"{code}\").property('latitude', {lat}).property('longitude', {lng}).property('city', '{city}').property('country', '{country}')";
                            var airport = new Airport
                            {
                                Code = code,
                                Name = city,
                                Coordinate = new GeoCoordinate(Convert.ToDouble(lat), Convert.ToDouble(lng))
                            };
                            airports.Add(code, airport);

                            IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinQuery);
                            count++;

                            await retryPolicy
                                .ExecuteAsync(async () =>
                                {
                                    System.Console.WriteLine($"{count} {gremlinQuery}");
                                    await query.ExecuteNextAsync();
                                });
                        }
                    }
                }

                var routesCsvStream = await httpClient.GetStreamAsync("https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat");
                using (var fileStream = new FileStream("routes.dat", FileMode.Create, FileAccess.Write))
                {
                    await routesCsvStream.CopyToAsync(fileStream);
                    routesCsvStream.Close();
                }

                using (var fileStream = new FileStream("routes.dat", FileMode.Open))
                using (var reader = new CsvReader(new StreamReader(fileStream)))
                {
                    var count = 0;
                    while (reader.Read())
                    {
                        var airline = reader.GetField(0);
                        var from = reader.GetField(2);
                        var to = reader.GetField(4);
                        var stops = reader.GetField(7);

                        if (string.IsNullOrEmpty(from) || string.IsNullOrEmpty(to))
                        {
                            continue;
                        }

                        var route = Tuple.Create(from, to);

                        airports.TryGetValue(from, out var fromAirport);
                        airports.TryGetValue(to, out var toAirport);

                        var isDirect = stops == "0";

                        if (isDirect && !routes.Contains(route) && fromAirport != null && toAirport != null)
                        {
                            routes.Add(route);
                            var distance = fromAirport.Coordinate.GetDistanceTo(toAirport.Coordinate);
                            var gremlinQuery = $"g.V('{fromAirport.Code}').addE('flight').to(g.V('{toAirport.Code}')).property('distance', {distance})";

                            IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinQuery);
                            count++;

                            await retryPolicy
                                .ExecuteAsync(async () =>
                                    {
                                        System.Console.WriteLine($"{count} {gremlinQuery}");
                                        await query.ExecuteNextAsync();
                                    });
                        }
                    }
                }
            }
        }
    }
}
