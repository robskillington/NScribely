using System;
using System.Threading;
using NScribely;

namespace NScribelyProducer
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			var wait = new ManualResetEvent(false);

			var producer = new Producer("127.0.0.1", 1463);
			producer.QueueFlushed += (sender, eventArgs) => wait.Set();
			producer.Send("default", "hello world from NScribely");

			wait.WaitOne();
		}
	}
}