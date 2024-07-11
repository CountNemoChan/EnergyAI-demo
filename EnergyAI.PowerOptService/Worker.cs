using CommonUntils;
using EnergyAI.PowerOptService.BLL;

namespace EnergyAI.PowerOptService
{
    public class Worker : ServiceBaseWorker
    {
        public Worker(): base("调优预测服务", Init, WorkerDispose)
        {            
        }

        static void Init()
        {
            Calculate.Init();
        }

        static void WorkerDispose()
        {
            TimerTasks.Disposable();
            LogHelper.Dispose();
        }
    }
}