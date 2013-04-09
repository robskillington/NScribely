using System;
using System.Collections;
using System.Collections.Generic;
using NScribely.Scribe;
using Thrift.Protocol;
using Thrift.Transport;
using Timer = System.Timers.Timer;

namespace NScribely
{
	public delegate void ProducerQueueFlushedEventHandler(object sender, EventArgs e);

	public class Producer
	{
		// Try to flush the queue every 200ms
		public const int DefaultFlushIntervalMs = 200;

		// 2^15: tested to roughly use ~45mb for messages roughly 600bytes long, 
		// a relatively good buffer if the receiver dies for a few hours
		public const int DefaultMaxQueueBuffer = 32768; 

		public Producer(string host, int port, int flushIntervalMs = DefaultFlushIntervalMs, int maxQueueBuffer = DefaultMaxQueueBuffer)
		{
			if (flushIntervalMs < 1)
			{
				throw new ArgumentException("not a positive integer", "flushIntervalMs");
			}

			FlushIntervalMs = flushIntervalMs;
			MaxQueueBuffer = maxQueueBuffer;
			Host = host;
			Port = port;
			Queue = Queue.Synchronized(new Queue());

			ScheduleFlush();
		}

		public string Host { get; private set; }

		public int Port { get; private set; }

		private Queue Queue { get; set; }

		private Timer Timer { get; set; }

		private int FlushIntervalMs { get; set; }

		private int MaxQueueBuffer { get; set; }

		public event ProducerQueueFlushedEventHandler QueueFlushed = (sender, args) => { };

		public bool TrySend(string category, string message)
		{
			if (Queue.Count >= MaxQueueBuffer)
			{
				return false;
			}

			Queue.Enqueue(new Item
				{
					Category = category,
					Message = message
				});

			return true;
		}

		private void ScheduleFlush()
		{
			Timer = new Timer
				{
					Interval = FlushIntervalMs,
					Enabled = true,
					AutoReset = false
				};

			Timer.Elapsed += (sender, args) => FlushQueue();
		}

		private void FlushQueue()
		{
			if (Queue.Count < 1)
			{
				ScheduleFlush();
				return;
			}

			var socket = new TSocket(Host, Port);
			var transport = new TFramedTransport(socket);
			var protocol = new TBinaryProtocol(transport, false, false);

			transport.Open();

			var client = new ScribeClient.Client(protocol);
			var retry = new List<Item>();

			while (Queue.Count > 0)
			{
				Item item;

				try
				{
					item = Queue.Dequeue() as Item;
				}
				catch (InvalidOperationException)
				{
					// No item available, while loop will ensure we fall out after this
					continue;
				}

				if (item == null)
				{
					continue;
				}

				try
				{
					client.Log(new List<LogEntry>
						{
							new LogEntry
								{
									Category = item.Category,
									Message = item.Message
								}
						});
				}
				catch (Exception)
				{
					// Retry next iteration
					retry.Add(item);
				}
			}

			transport.Close();

			// Add to the queue items to retry
			foreach (var item in retry)
			{
				Queue.Enqueue(item);
			}

			QueueFlushed(this, EventArgs.Empty);

			ScheduleFlush();
		}
	}
}