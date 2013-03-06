using NScribely;

namespace NScribelyProducer
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			var producer = new Producer("127.0.0.1", 1463);
			producer.Send("default", "hello world from NScribely");
		}
	}
}