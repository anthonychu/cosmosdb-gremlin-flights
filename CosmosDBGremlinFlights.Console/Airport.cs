using System.Device.Location;

namespace CosmosDBGremlinFlights.Console
{
    internal class Airport
    {
        public string Code { get; internal set; }
        public string Name { get; internal set; }
        public GeoCoordinate Coordinate { get; internal set; }
    }
}