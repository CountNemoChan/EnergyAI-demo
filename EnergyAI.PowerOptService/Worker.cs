using CommonUntils;
using EnergyAI.PowerOptService.BLL;

namespace EnergyAI.PowerOptService
{
    public class Worker : ServiceBaseWorker
    {
        public Worker(): base("����Ԥ�����", Init, WorkerDispose)
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