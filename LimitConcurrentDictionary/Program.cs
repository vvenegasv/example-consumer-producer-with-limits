using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LimitConcurrentDictionary
{
    class Program
    {
        private static readonly int MaxRunningElements = 40;
        private static readonly Random RandomWork = new Random(DateTime.Now.Millisecond);
        private static readonly ConcurrentDictionary<int, string> Running = new ConcurrentDictionary<int, string>(MaxRunningElements, 999999);
        private static readonly ConcurrentQueue<int> Queue = new ConcurrentQueue<int>();
        private static readonly ConcurrentBag<int> Finished = new ConcurrentBag<int>();
        private static readonly ConcurrentBag<string> Messages = new ConcurrentBag<string>();
        private static readonly ConcurrentBag<string> Errors = new ConcurrentBag<string>();
        private static readonly ConcurrentBag<Task> Consumers = new ConcurrentBag<Task>();
        private static readonly ConcurrentBag<Task> Producers = new ConcurrentBag<Task>();

        static void Main(string[] args)
        {
            Run();
            Console.ReadKey();
        }

        static void Run()
        {
            ShowStatus();
            Produce();
            Consume();
            
            while (Running.Any() || Queue.Any() || !Finished.Any())
            {
                Thread.Sleep(1000);
            }
        }
        
        
        static void ShowStatus(bool lastIteration = false)
        {
            Task.Factory.StartNew(() =>
            {
                Console.Clear();
                Console.WriteLine("");
                Console.WriteLine("\t============================================================================");
                Console.WriteLine("\t\t\tEstado del Proceso\t{0}", DateTime.Now.ToString("HH:mm:ss.fff"));
                Console.WriteLine("\t============================================================================");
                Console.WriteLine("");
                Console.WriteLine("\tElementos En Cola: \t{0}", Queue.Count);
                Console.WriteLine("\tElementos En Proceso: \t{0}", Running.Count);
                Console.WriteLine("\tElementos Procesados: \t{0}", Finished.Count);
                
                foreach (var status in Producers.Select(x => x.Status).Distinct())
                {
                    Console.WriteLine("\tProductores {0} \t: {1}", status, Producers.Where(x => x.Status == status).Count());
                }

                foreach (var status in Consumers.Select(x => x.Status).Distinct())
                {
                    Console.WriteLine("\tConsumidores {0} \t: {1}", status, Consumers.Where(x => x.Status == status).Count());
                }
               
                foreach (var consumerTask in Consumers.Where(x => x.Status == TaskStatus.Running))
                {
                    Console.WriteLine("\t \t ConsumerId:{0}, Status:{1}", consumerTask.Id, consumerTask.Status);
                }
                
                Console.WriteLine("\tErrores: \t\t{0}", Errors.Count);
                foreach (var err in Errors)
                    Console.WriteLine("\t \t - {0}", err);

                Console.WriteLine("\tMensajes: \t\t{0}", Messages.Count);
                foreach (var message in Messages.Take(10))
                    Console.WriteLine("\t \t - {0}", message);

                Console.WriteLine("");

                if(lastIteration)
                    Console.WriteLine("\tProceso finalizado. Presione cualquier tecla para cerrar la aplicaicón");
                else
                    Console.WriteLine("\tProceso en ejecución. presione cualquier tecla para abortarlo.");
                Console.WriteLine("\t============================================================================");
                Thread.Sleep(500);

                if (Queue.Any() || Running.Any() || !Finished.Any())
                {
                    ShowStatus();
                }
                else
                {
                    if (!lastIteration)
                        ShowStatus(true);
                }
            });
        }

        static void Produce()
        {
            for (int i = 0; i < 8; i++)
            {
                var startFrom = i*300;
                for (int j = startFrom; j < startFrom + 300; j++)
                {
                    var item = j;
                    var productorTask = Task.Factory.StartNew(() =>
                    {
                        Queue.Enqueue(item);
                        Thread.Sleep(RandomWork.Next(10, 30));
                    });
                    Producers.Add(productorTask);
                }
            }
        }

        private static void Consume()
        {
            int item;
            int throtledCount = 0;
            while (true)
            {
                while (Queue.TryDequeue(out item))
                {
                    if (Running.Count >= MaxRunningElements)
                    {
                        throtledCount++;
                        Queue.Enqueue(item);
                        Thread.Sleep(throtledCount * 50);
                        Messages.Add(string.Format("{0}: ThrottledCount={1} SleepFor={2}", DateTime.Now.ToString("HH:mm:ss.fff"), throtledCount, throtledCount * 50));
                        break;
                    }
                    else
                        throtledCount = 0;

                    //Messages.Add(string.Format("{0}: dequeue", DateTime.Now.ToString("HH:mm:ss.fff")));
                    ProcessConsumedItem(item);
                }

                if (!Queue.Any())
                    Thread.Sleep(3000);
            }
        }

        private static void ProcessConsumedItem(int item)
        {

            if (!Running.TryAdd(item, item.ToString()))
            {
                Queue.Enqueue(item);
                Errors.Add(string.Format("{0}: No se pudo agregar el item {1} al listado de ejecutados. Se encoló nuevamente.", DateTime.Now.ToString("HH:mm:ss.fff"), item));
            }
            else
            {
                var task = new Task(async () =>
                {
                    //Simulate some work
                    await Task.Delay(RandomWork.Next(300, 3000));
                    Finished.Add(item);
                    DeleteFromRunning(item);
                });
                Consumers.Add(task);
                //Messages.Add(String.Format("{0}: Id={1} Status:{2} Where=\"BeforeStart\"", DateTime.Now.ToString("HH:mm:ss.fff"),  task.Id, task.Status));
                task.Start();
                //Messages.Add(String.Format("{0}: Id={1} Status:{2} Where=\"Started\"", DateTime.Now.ToString("HH:mm:ss.fff"), task.Id, task.Status));
            }
        }
        
        static void DeleteFromRunning(int key, int retryCount = 0)
        {
            string value;
            if (retryCount > 5)
            {
                Errors.Add(string.Format("No se pudo eliminar el elemento {0} del diccionario de ejecución", key));
            }

            if (!Running.TryRemove(key, out value))
                DeleteFromRunning(key, retryCount + 1);
        }
 
    }
}
