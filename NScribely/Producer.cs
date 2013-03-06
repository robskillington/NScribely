using System.Collections.Generic;
using NScribely.Scribe;
using Thrift.Protocol;
using Thrift.Transport;

namespace NScribely
{
	public class Producer
	{
		public Producer(string host, int port)
		{
			Host = host;
			Port = port;
		}

		public string Host { get; private set; }

		public int Port { get; private set; }

		public Producer Send(string category, string message)
		{
			var socket = new TSocket(Host, Port);
			var transport = new TFramedTransport(socket);
			var protocol = new TBinaryProtocol(transport, false, false);

			transport.Open();

			var client = new ScribeClient.Client(protocol);

			client.Log(new List<LogEntry>
				{
					new LogEntry
						{
							Category = category,
							Message = message
						}
				});

			transport.Close();

			return this;
		}
	}
}