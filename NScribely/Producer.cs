using System;
using System.Collections;
using System.Collections.Generic;
using System.Timers;
using NScribely.Scribe;
using Thrift.Protocol;
using Thrift.Transport;

namespace NScribely
{
	public delegate void ProducerQueueFlushedEventHandler(object sender, EventArgs e);

	public class Producer
	{
		public const int DefaultFlushInterval = 1000;

		public Producer(string host, int port, int flushInterval = DefaultFlushInterval)
		{
			if (flushInterval < 1)
			{
				throw new ArgumentException("not a positive integer", "flushInterval");
			}

			Host = host;
			Port = port;
			Queue = Queue.Synchronized(new Queue());
			Timer = new Timer
				{
					Interval = flushInterval,
					Enabled = true
				};

			Timer.Elapsed += (sender, args) => FlushQueue();
		}

		public string Host { get; private set; }

		public int Port { get; private set; }

		private Queue Queue { get; set; }

		private Timer Timer { get; set; }
		public event ProducerQueueFlushedEventHandler QueueFlushed;

		public Producer Send(string category, string message)
		{
			Queue.Enqueue(new Item
				{
					Category = category,
					Message = message
				});

			return this;
		}

		private void FlushQueue()
		{
			var socket = new TSocket(Host, Port);
			var transport = new TFramedTransport(socket);
			var protocol = new TBinaryProtocol(transport, false, false);

			transport.Open();

			var client = new ScribeClient.Client(protocol);

			while (Queue.Count > 0)
			{
				try
				{
					var item = Queue.Dequeue() as Item;

					client.Log(new List<LogEntry>
						{
							new LogEntry
								{
									Category = item.Category,
									Message = item.Message
								}
						});

					Console.WriteLine("Sent log");
				}
				catch (InvalidOperationException)
				{
					// No item available, while loop will ensure we fall out after this
				}
			}

			transport.Close();

			QueueFlushed(this, EventArgs.Empty);
		}
	}
}