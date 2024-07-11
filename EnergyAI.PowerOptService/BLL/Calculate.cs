using CommonUntils;
using EnergyAi.Base;
using EnergyAi.Build;
using EnergyAi.WEA;
using EnergyAI.AILearn.DBHelper;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Data;
using Z.EntityFramework.Plus;

namespace EnergyAI.PowerOptService.BLL
{
    /// <summary>
    /// This class is used to calculate the power consumption of the buildings.
    /// </summary>
    public static class Calculate
    {
        const int HTTP_RETRY_NUM = 2;//http请求重试次数
        const string WEA_VAR_CODE = "IsHoliday";//WEA变量代码
        static int loopRealTimeMs = 30 * 1000;//实时计算间隔
        static int loopForecastMs = 60 * 1000;//预测计算间隔
        static int runMode = 0; //运行模式，1-经济模式，2-安全模式
        //static List<Wea_PhotoVoltaics> _allPhotoVoltaics = new List<Wea_PhotoVoltaics>();
        static List<View_PowerForecast> _curPowerForecast = new List<View_PowerForecast>();//当前实时电量预测

        static List<View_AircondStatus> _curAircondStatus = new List<View_AircondStatus>();//当前空调状态

        static List<Base_ParkInfo> _allParkInfos = new List<Base_ParkInfo>();//停车场信息
        static List<Build_BuildInfo> _allBuildInfos = new List<Build_BuildInfo>();//建筑信息
        static List<Build_BuildControlType> _allBuildControlTypes = new List<Build_BuildControlType>();//建筑控制器类型

        // db helper
        static EFDbHelper<BuildDbContext> _dbHelper = new EFDbHelper<BuildDbContext>(() => { return BuildDbContext.GetDbContext(); });


        /// <summary>
        /// 初始化数据
        /// </summary>
        public static void Init()
        {

            DataInit();//初始化数据

            TimerTasks.AddTimerTask("系统调优", loopForecastMs, (lpms) =>
            {
                CalcOpt();
            }, true);//启动系统调优任务

        }
        /// <summary>
        ///  调控优化
        /// </summary>
        static public void CalcOpt()
        {
            using (var db = _dbHelper.GetDbContext())
            {
                //string strCurDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd");
                //int iCurHour =  DateTime.Now.AddHours(1).Hour;
                // _curPowerForecast = db.Set<View_PowerForecast>().Where(t => t.Predate == strCurDate && t.PreHour == iCurHour).ToList();
                //_curAircondStatus =db.Set<View_AircondStatus>().OrderBy(t=>t.WeaVarCode).ToList();

                string strOptTime = "";// 优化时间
                string strCurrentDate = DateTime.Now.AddHours(0).ToString("yyyy-MM-dd");//当前日期

                //forcast next 15 minutes
                int iCurHour = DateTime.Now.Hour;//当前小时
                int iMinute = DateTime.Now.Minute;//当前分钟

                //if(iMinute>=45) strOptTime = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH") + ' '+ ":45";
                // 如果当前分钟大于等于45，则调控时间为当前小时的45分钟
                if (iMinute >= 45) strOptTime = strCurrentDate + ' ' + iCurHour.ToString() + ":45";
                // 如果当前分钟大于等于30，且小于45，则调控时间为当前小时的30分钟
                if (iMinute >= 30 && iMinute < 45) strOptTime = strCurrentDate + ' ' + iCurHour.ToString() + ":30";
                // 如果当前分钟大于等于15，且小于30，则调控时间为当前小时的15分钟
                if (iMinute >= 15 && iMinute < 30) strOptTime = strCurrentDate + ' ' + iCurHour.ToString() + ":15";
                // 如果当前分钟大于等于0，且小于15，则调控时间为当前小时的0分钟
                if (iMinute >= 0 && iMinute < 15) strOptTime = strCurrentDate + ' ' + iCurHour.ToString() + ":00";

                //get the temperature of outside
                // 获取当前温度脚本
                string sqlGetTemperature = string.Format("   SELECT  TOP 1  [BuildInfoId] ,[BuildControlTypeId]     ,[WeaVarCode]     ,[IsTxtOfValue]     ,[DecimalValue]     ,[TxtValue]     ,WeaTime   FROM  [EnergyAI].[dbo].[Build_CalcForecast]  " +
                    "  where WeaVarCode='Temperature'AND (CONVERT(varchar(10),  WeaTime, 120) = CONVERT(varchar(10), GETDATE(), 120)) AND (DATEPART(HOUR, WeaTime) = DATEPART(HOUR,GETDATE()))  " +
                    "ORDER BY  WeaTime desc,BuildInfoId,BuildControlTypeId,WeaVarCode");

                DataTable dtOutSideTemperature = db.Database.SqlQuery(sqlGetTemperature);
                decimal dTemperature = 0;
                if (dtOutSideTemperature.Rows.Count > 0)
                {

                    dTemperature = decimal.Parse(dtOutSideTemperature.Rows[0]["DecimalValue"].ToString()!);

                    LogHelper.AddInfo("系统调优", $"室外温度【{dTemperature}】度");
                }

                //LogHelper.AddInfo("View_PowerForecast", $"获取实时预测数据【{string.Join(",", _curPowerForecast.Select(t => t.PreValue).Distinct().ToList())}】");
                //LogHelper.AddInfo("空调调优", $"获取预测数据共【{_curPowerForecast.Count}】条");



                decimal dPowerLimit = 0, dCountryPowerLimit = 0;

                // 功率名称 功率值(kW)
                //市电功率    5774.20
                //国网Limit功率   3000.00
                //未来15分钟光伏预测功率    4193.77
                //当前光伏功率  2385.23

                //获取预测发电量和楼宇使用量脚本
                string sql_getPowerLimit = string.Format("SELECT  [功率名称] AS PowerName,ISNULL([功率值(kW)],0) as PowerValue FROM ControlCorrelationPower ");

                //string sql_getPowerLimit = string.Format("SELECT  '市电功率' AS PowerName,6000 as PowerValue UNION ALL  SELECT  '国网Limit功率' AS PowerName,0 as PowerValue UNION ALL SELECT  '未来15分钟光伏预测功率' AS PowerName, 5000 as PowerValue UNION ALL SELECT  '当前光伏功率' AS PowerName, 7000 as PowerValue ");

                DataTable dtPowerLimit = db.Database.SqlQuery(sql_getPowerLimit);
                //SELECT SUM([预测未来一小时楼宇功率]) AS NextBuildPower FROM[BuildReport]

                //select[未来一小时AI预计发电量] as NextPVPower FROM[EnergyAI].[dbo].[report]
                LogHelper.AddInfo("Power_Opt", $"调优,获取预测发电量和楼宇使用量！");

                decimal dNextBuildPower = 0, dNextPVPower = 0, dCityPower = 0, dCurPVPower = 0, dAdjustPower = 0;
                //string sqlNextBuildPower = string.Format("SELECT [实际楼宇功率] as RealPower ,ISNULL([预测未来一小时楼宇功率],0) AS NextBuildPower FROM[BuildReport] where BuildName='园区总负荷' ");

                string sqlNextBuildPower = string.Format("SELECT ISNULL(SUM([未来15分钟楼宇功率]),0) AS NextBuildPower FROM[BuildReport] where typeName='总值' ");
                //string sqlNextBuildPower = string.Format("SELECT 6500 AS NextBuildPower ");

                DataTable dtNextBuildPower = db.Database.SqlQuery(sqlNextBuildPower);
                if (dtNextBuildPower.Rows.Count > 0)
                {
                    dNextBuildPower = decimal.Parse(dtNextBuildPower.Rows[0]["NextBuildPower"].ToString()!);
                    //dRealPowerOfPark = decimal.Parse(dtNextBuildPower.Rows[0]["RealPower"].ToString()!);
                }


                //dNextPVPower = decimal.Parse(dtPowerLimit.Rows[2][1].ToString()!);




                if (dtPowerLimit.Rows.Count > 0)
                {
                    //--PowerName PowerValue
                    //--市电功率  1841.00
                    //--国网Limit功率 2475.00
                    //--未来15分钟光伏预测功率   0.00
                    //--当前光伏功率    0.00

                    dPowerLimit = decimal.Parse(dtPowerLimit.Rows[0][1].ToString()!) - decimal.Parse(dtPowerLimit.Rows[1][1].ToString()!);//当前市电功率减去国网限值功率，得到可调节的功率

                    //dPowerLimit = decimal.Parse(dtPowerLimit.Rows[0][1].ToString()!) - decimal.Parse(dtPowerLimit.Rows[1][1].ToString()!) - (decimal.Parse(dtPowerLimit.Rows[2][1].ToString()!) - decimal.Parse(dtPowerLimit.Rows[3][1].ToString()!));

                    dCityPower = decimal.Parse(dtPowerLimit.Rows[0][1].ToString()!);
                    dCountryPowerLimit = decimal.Parse(dtPowerLimit.Rows[1][1].ToString()!);
                    dNextPVPower = decimal.Parse(dtPowerLimit.Rows[2][1].ToString()!);
                    dCurPVPower = decimal.Parse(dtPowerLimit.Rows[3][1].ToString()!);
                    dAdjustPower = dPowerLimit * 1.1M;
                    LogHelper.AddInfo("Power_Opt", $"调优,获取预测(15分钟)发电量【{dNextPVPower}】和楼宇使用量【{dNextBuildPower}】！");
                    LogHelper.AddInfo("Power_Opt", $"【调优】市电功率:【{dtPowerLimit.Rows[0][1].ToString()}】");
                    LogHelper.AddInfo("Power_Opt", $"【调优】国网限电功率:【{dCountryPowerLimit}】");
                    LogHelper.AddInfo("Power_Opt", $"【调优】未来15分钟光伏预测功率:【{dtPowerLimit.Rows[2][1].ToString()}】");
                    LogHelper.AddInfo("Power_Opt", $"【调优】当前光伏功率:【{dtPowerLimit.Rows[3][1].ToString()}】");
                    LogHelper.AddInfo("Power_Opt", $"【调优】调优总功率:【{dPowerLimit}】-->【{dAdjustPower}】(加10%)");
                }
                if (dCountryPowerLimit > 0) //有国网限值，执行顺序逻辑调优
                {
                    //&& dPowerLimit > 0
                    //国网如需要限电,可调控功率=市电功率-国网限制值-（未来一小时光伏预测功率-当前光伏功率），然后再逐步扣减，扣完为止
                    //如未扣完则表示已经无可调控设备

                    if (dPowerLimit > 0)   //有国网限制下，公式计算值>0表示电量不够需要进行调优
                    {
                        strOptTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:00");//调整当前时间

                        AdjustOpt(strOptTime, dAdjustPower, dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, dAdjustPower);
                    }
                    else //有国网限制下，公式计算值<0进行余电上网
                        AdjustOpt(strOptTime, dAdjustPower, dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, dAdjustPower);

                }
                else //无国网限值，执行经济模式
                {
                    /*1.发电量>用电量
                     * (1) 因为此时必须用光伏发电,因此记录余电上网
                     * (2)记录当前余电上网电价
                     * 2.发电量<用电量
                     * (1)判断当前时间的电价比较,如国网电价<储能电价,则用国网电记录国网电价,否则释放储能并记录光伏电价
                     * (2)进行根据尖峰谷时段空调调优,根据区间电价,气温,历史空调最优温度等数据计算空调最佳温度进行调优
                     * (3)充电桩调优,降低10%
                     * (4)记录光伏电价或国网电价
                    */
                    //forcast pv power and needed power
                    //AdjustAirconditionsByEcnomic(strOptTime);
                    dPowerLimit = (dNextPVPower - dNextBuildPower);
                    dAdjustPower = dPowerLimit;
                    if (dPowerLimit >= 0)//发电量大于耗电量
                    {
                        dAdjustPower = dPowerLimit * 0.9M;
                        SavePowerOpt(strOptTime, dAdjustPower, dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, dAdjustPower); //if not enought power

                    }
                    else //if has enought power 发电量小于耗电量,执行经济模式调优
                    {
                        dAdjustPower = dPowerLimit * 1.1M;
                        EcnomicPowerOpt(strOptTime, Math.Abs(dAdjustPower), dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, Math.Abs(dAdjustPower));
                    }


                }


            }
        }

        static void AdjustOpt(string strOptTime, decimal dPowerLimit, decimal dCountryPowerLimit, decimal dNextPVPower, decimal dNextBuildPower, decimal dCityPower, decimal dCurPVPower, decimal dAdjustPower)
        {

            using (var db = _dbHelper.GetDbContext())
            {
                //    string strCurDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd");
                //int iCurHour = DateTime.Now.AddHours(1).Hour;
                //    string strDeleteDate2 = strCurDate + ' ' + iCurHour.ToString() + ":00:00";
                //    strDeleteDate2=DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH:mm");
                string strDeleteDate2 = strOptTime;
                LogHelper.AddInfo("Power_Opt", $"【调优】预测时间:【每15分钟】-->【{strOptTime}】");
                //delete the history opt data
                db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}'", strOptTime));
                //decimal dCountryPowerLimit = dPowerLimit;
                //db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, "交流仓"));
                //db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, "直流仓"));

                if (dPowerLimit > 0)
                {
                    decimal dAdviseValue = 0;
                    //1.release the ac storage
                    /* [储能]
                       ,[状态]
                       ,[储能当前功率]
                       ,[电池组SOC]
                       ,[储能SOC上限]
                       ,[储能SOC下限]qq
                     */
                    string strACStatus = "";
                    string sqlACStatus = string.Format("SELECT  [储能],[状态],[储能当前功率] ,[电池组SOC] ,[储能SOC上限]  ,[储能SOC下限]  FROM [EnergyAI].[dbo].[ChargeStatus] where 储能='交流仓'");
                    // string sqlACStatus = string.Format(" SELECT '交流仓' [储能],'就绪' [状态], -0.5 [储能当前功率] ,93 [电池组SOC] ,94 [储能SOC上限]  ,12 [储能SOC下限] ");

                    decimal dSavePower = 0;
                    DataTable dtAcStatus = db.Database.SqlQuery(sqlACStatus);
                    if (dtAcStatus.Rows.Count > 0)
                    {
                        strACStatus = dtAcStatus.Rows[0]["状态"].ToString()!;
                        decimal dCurPower = decimal.Parse(dtAcStatus.Rows[0]["储能当前功率"].ToString()!);
                        decimal dSOCVol = decimal.Parse(dtAcStatus.Rows[0]["电池组SOC"].ToString()!);
                        decimal dSOCVolUpper = decimal.Parse(dtAcStatus.Rows[0]["储能SOC上限"].ToString()!);
                        decimal dSOCVolLower = decimal.Parse(dtAcStatus.Rows[0]["储能SOC下限"].ToString()!);
                        if (dSOCVol > dSOCVolLower)
                        {
                            if (strACStatus == "充电")
                            {
                                if (dPowerLimit >= dCurPower)
                                {
                                    //如果是充电状态下，需要进行调控的功率减去交流仓充电功率求得当前剩余功率
                                    var sy = dPowerLimit - dCurPower;
                                    //目前交流仓变成就绪状态，但是调控功率还有剩余，需要将交流仓改为放电状态
                                    if (sy >= 350)
                                    {
                                        dSavePower = 350;
                                        dPowerLimit = sy - dSavePower;
                                    }
                                    else
                                    {
                                        dPowerLimit = 0;
                                        dSavePower = sy;
                                    }
                                    dAdviseValue = dSavePower;
                                }
                                if (dPowerLimit < dCurPower)
                                {
                                    dSavePower = dCurPower - dPowerLimit;
                                    dAdviseValue = -dSavePower;
                                    dPowerLimit = 0;
                                }
                            }
                            if (strACStatus == "放电")
                            {
                                //交流仓最大400功率，减去当前交流仓功率，得到剩余的调控功率

                                var syktkgl = 350 - dCurPower;
                                if (dPowerLimit >= syktkgl)
                                {
                                    dPowerLimit = dPowerLimit - syktkgl;
                                    dAdviseValue = 350;
                                }
                                else
                                {
                                    dAdviseValue = dCurPower + dPowerLimit;
                                    dPowerLimit = 0;
                                }
                            }

                            if (strACStatus == "就绪")
                            {
                                if (dPowerLimit >= 350)
                                {
                                    dSavePower = 350;
                                    dPowerLimit = dPowerLimit - dSavePower;
                                    dAdviseValue = dSavePower;
                                }
                                else
                                {
                                    dSavePower = dPowerLimit;
                                    dPowerLimit = 0;    xixi b
                                    dAdviseValue = dSavePower;
                                }
                            }
                        }
                        if (strACStatus == "充电" || strACStatus == "就绪" || strACStatus == "放电")
                        {

                            List<Power_Opt> curACPowerOpt = new List<Power_Opt>();
                            curACPowerOpt.Add(new Power_Opt()
                            {
                                RunMode = runMode,
                                //BuildInfoId = "--",//pf.BuildInfoId,
                                //BuildControlTypeId = "--",//pf.BuildControlTypeId,DateTime.Now.ToString("yyyy-MM-dd HH:00:00")
                                OptTime = DateTime.Parse(strDeleteDate2),
                                VarCode = "交流仓",
                                RealValue = strACStatus == "充电" ? -dCurPower : dCurPower,
                                OptValue = dAdviseValue,
                                RowStatus = "Y",
                                PowerLimit = dCountryPowerLimit,
                                NextPVPower = dNextPVPower,
                                NextBuildPower = dNextBuildPower,
                                CityPower = dCityPower,
                                CurPVPower = dCurPVPower,
                                AdjustPower = dAdjustPower,
                                Remark = string.Format("Status:{0},Vol:{1},Upper:{2},Lower:{3}", strACStatus, dSOCVol, dSOCVolUpper, dSOCVolLower),
                                Id = Guid.NewGuid()
                            });

                            int iDelCount = _dbHelper.InsertList<Power_Opt>(curACPowerOpt, null);
                            LogHelper.AddInfo("Power_Opt", $"新增调优数据:【交流仓】,调优值:【{dCurPower}】-->【{dAdviseValue}】");

                        }
                    }
                    //2.release the dc storage
                    dAdviseValue = 0;
                    if (dPowerLimit > 0)
                    {
                        string strDCStatus = "";
                        string sqlDCStatus = string.Format("SELECT  [储能],[状态],[储能当前功率] ,[电池组SOC] ,[储能SOC上限]  ,[储能SOC下限]  FROM [EnergyAI].[dbo].[ChargeStatus] where 储能='直流仓'");
                        //string sqlDCStatus = string.Format(" SELECT '直流仓' [储能],'就绪' [状态], 0 [储能当前功率] ,91 [电池组SOC] ,92 [储能SOC上限]  ,20 [储能SOC下限] ");

                        DataTable dtDCStatus = db.Database.SqlQuery(sqlDCStatus);
                        if (dtDCStatus.Rows.Count > 0)
                        {
                            strDCStatus = dtDCStatus.Rows[0]["状态"].ToString()!;
                            decimal dDCCurPower = decimal.Parse(dtDCStatus.Rows[0]["储能当前功率"].ToString()!);
                            decimal dSOCVol = decimal.Parse(dtDCStatus.Rows[0]["电池组SOC"].ToString()!);
                            decimal dSOCVolUpper = decimal.Parse(dtDCStatus.Rows[0]["储能SOC上限"].ToString()!);
                            decimal dSOCVolLower = decimal.Parse(dtDCStatus.Rows[0]["储能SOC下限"].ToString()!);
                            if (dSOCVol > dSOCVolLower)
                            {
                                if (strDCStatus == "充电")
                                {
                                    if (dPowerLimit >= dDCCurPower)
                                    {
                                        //如果是充电状态下，需要进行调控的功率减去交流仓充电功率求得当前剩余功率
                                        var sy = dPowerLimit - dDCCurPower;
                                        //目前交流仓变成就绪状态，但是调控功率还有剩余，需要将交流仓改为放电状态
                                        if (sy >= 200)
                                        {
                                            dSavePower = 200;
                                            dPowerLimit = sy - dSavePower;
                                        }
                                        else
                                        {
                                            dPowerLimit = 0;
                                            dSavePower = sy;
                                        }
                                        dAdviseValue = dSavePower;
                                    }
                                    if (dPowerLimit < dDCCurPower)
                                    {
                                        dSavePower = dDCCurPower - dPowerLimit;
                                        dAdviseValue = -dSavePower;
                                        dPowerLimit = 0;
                                    }
                                }
                                if (strDCStatus == "放电")
                                {
                                    //直流仓最大200功率减去当前直流仓功率，得到剩余的调控功率

                                    var syktkgl = 200 - dDCCurPower;
                                    if (dPowerLimit >= syktkgl)
                                    {
                                        dPowerLimit = dPowerLimit - syktkgl;
                                        dAdviseValue = 200;
                                    }
                                    else
                                    {
                                        dAdviseValue = dDCCurPower + dPowerLimit;
                                        dPowerLimit = 0;
                                    }
                                }

                                if (strDCStatus == "就绪")
                                {
                                    if (dPowerLimit >= 200)
                                    {
                                        dSavePower = 200;
                                        dPowerLimit = dPowerLimit - dSavePower;
                                        dAdviseValue = dSavePower;
                                    }
                                    else
                                    {
                                        dSavePower = dPowerLimit;
                                        dPowerLimit = 0;
                                        dAdviseValue = dSavePower;
                                    }

                                }
                            }
                            db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, "直流仓"));
                            // if (strDCStatus == "充电" || strDCStatus == "就绪" || strDCStatus == "放电")
                            {

                                List<Power_Opt> curDCPowerOpt = new List<Power_Opt>();
                                curDCPowerOpt.Add(new Power_Opt()
                                {
                                    RunMode = runMode,
                                    //BuildInfoId = "--",//pf.BuildInfoId,
                                    //BuildControlTypeId = "--",//pf.BuildControlTypeId,
                                    OptTime = DateTime.Parse(strDeleteDate2),
                                    VarCode = "直流仓",
                                    RealValue = dDCCurPower,
                                    OptValue = dAdviseValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    Remark = string.Format("Status:{0},Vol:{1},Upper:{2},Lower:{3}", strDCStatus, dSOCVol, dSOCVolUpper, dSOCVolLower),
                                    Id = Guid.NewGuid()
                                });

                                int iDelCount = _dbHelper.InsertList<Power_Opt>(curDCPowerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据:【直流仓】,调优值:【{dDCCurPower}】-->【{dAdviseValue}】");
                            }
                        }
                    }
                    //3.release the  charger
                    //string sql_getStations = string.Format("SELECT distinct StationID,StationName FROM [EnergyAI].[dbo].[Charger_StationInfo] order by StationID");

                    //DataTable dtStations = db.Database.SqlQuery(sql_getStations);
                    //foreach (DataRow dr in dtStations.Rows)
                    //{
                    //    string? strStationId = dr["StationID"].ToString();
                    //    string? strStationName = dr["StationName"].ToString();
                    //    db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, strStationName));
                    //}
                    if (dPowerLimit > 0) // ChargeStation
                    {

                        double dTotalPower = 0;
                        string sql_getStations = string.Format("SELECT distinct StationID,StationName FROM [EnergyAI].[dbo].[Charger_StationInfo] order by StationID");

                        DataTable dtStations = db.Database.SqlQuery(sql_getStations);
                        foreach (DataRow dr in dtStations.Rows)
                        {
                            string? strStationId = dr["StationID"].ToString();
                            string? strStationName = dr["StationName"].ToString();
                            //ChargeStationView

                            decimal dRealPower = 0;
                            string sqlRealPower = string.Format("SELECT [StationID] ,[StationName] ,[实际功率],[总电量] FROM [EnergyAI].[dbo].[ChargeStationView] where StationID='{0}'  ", strStationId);

                            DataTable dtRealPower = db.Database.SqlQuery(sqlRealPower);
                            dRealPower = decimal.Parse(dtRealPower.Rows[0]["实际功率"].ToString()!);
                            if (dRealPower > 0)
                            {
                                dAdviseValue = dRealPower * 0.9M;
                                dPowerLimit = dPowerLimit - dAdviseValue * 0.1M;
                                if (strStationName == "不对外开放-低压充电组" && dAdviseValue < 100)
                                    break;
                                if (strStationName == "不对外开放-高压充电组" && dAdviseValue < 40)
                                    break;
                                if (strStationName == "不对外开放-低压行政楼东" && dAdviseValue < 80)
                                    break;
                                if (strStationName == "不对外开放-中压充电组" && dAdviseValue < 185)
                                    break;
                                if (strStationName == "不对外开放-ABB不对外开放-中压充电组（2）" && dAdviseValue < 12)
                                    break;

                                List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                                curChargerOpt.Add(new Power_Opt()
                                {
                                    RunMode = runMode,
                                    //BuildInfoId = "--",//pf.BuildInfoId,
                                    // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                                    OptTime = DateTime.Parse(strDeleteDate2),
                                    RealValue = dRealPower,
                                    VarCode = strStationName,
                                    OptValue = dAdviseValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    Id = Guid.NewGuid()
                                });
                                //db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, strStationName));
                                int iDelCount = _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据:【{strStationName}】,调优值:【{dRealPower}】-->【{dAdviseValue}】");
                            }

                        }

                    }//end of ChargeStation Adjustion

                    if (dPowerLimit > 0) //Adjust the airconditions
                    {
                        if (dCountryPowerLimit > 0)
                            AdjustAirconditionsByLimit(strOptTime, 0, dAdjustPower, dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, dAdjustPower);

                    }

                }
                else //电量有多余，进行余电上网
                {
                    //获取当前电价
                    decimal dPowerPrice = 0;
                    string sqlPowerPrice = "SELECT * from Base_PowerPrice where PriceTime='-2' ";
                    DataTable dtPowerPrice = db.Database.SqlQuery(sqlPowerPrice);
                    dPowerPrice = decimal.Parse(dtPowerPrice.Rows[0]["Price"].ToString()!);

                    List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                    curChargerOpt.Add(new Power_Opt()
                    {
                        RunMode = 2,
                        //BuildInfoId = "--",//pf.BuildInfoId,
                        // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                        OptTime = DateTime.Parse(strDeleteDate2),
                        RealValue = 0,
                        VarCode = "余电上网",
                        OptValue = 0,
                        RowStatus = "Y",
                        PowerLimit = dCountryPowerLimit,
                        NextPVPower = dNextPVPower,
                        NextBuildPower = dNextBuildPower,
                        CityPower = dCityPower,
                        CurPVPower = dCurPVPower,
                        AdjustPower = dAdjustPower,
                        PowerPrice = dPowerPrice,
                        Id = Guid.NewGuid()
                    });
                    _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                    LogHelper.AddInfo("Power_Opt", $"新增调优数据:【余电上网】,调优值:【{dAdjustPower}】");
                }


            }
        }
        static void SavePowerOpt(string strOptTime, decimal dPowerLimit, decimal dCountryPowerLimit, decimal dNextPVPower, decimal dNextBuildPower, decimal dCityPower, decimal dCurPVPower, decimal dAdjustPower)
        {

            using (var db = _dbHelper.GetDbContext())
            {
                //db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' ", strOptTime));
                //计算电费和余电上网等信息
                //获取当前电价
                decimal dPowerPrice = 0;
                string sqlPowerPrice = "SELECT * from Base_PowerPrice where PriceTime='-2' ";
                DataTable dtPowerPrice = db.Database.SqlQuery(sqlPowerPrice);
                dPowerPrice = decimal.Parse(dtPowerPrice.Rows[0]["Price"].ToString()!);

                List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                curChargerOpt.Add(new Power_Opt()
                {
                    RunMode = 2,
                    //BuildInfoId = "--",//pf.BuildInfoId,
                    // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                    OptTime = DateTime.Parse(strOptTime),
                    RealValue = 0,
                    VarCode = "余电上网",
                    OptValue = 0,
                    RowStatus = "Y",
                    PowerLimit = dCountryPowerLimit,
                    NextPVPower = dNextPVPower,
                    NextBuildPower = dNextBuildPower,
                    CityPower = dCityPower,
                    CurPVPower = dCurPVPower,
                    AdjustPower = dAdjustPower,
                    PowerPrice = dPowerPrice,
                    Id = Guid.NewGuid()
                });
                db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strOptTime, "余电上网"));
                _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                LogHelper.AddInfo("Power_Opt", $"新增调优数据:【余电上网】,调优值:【{dAdjustPower}】");

                if (dPowerLimit > 0) // 有余电后进行充电桩调节增加10%的功率
                {

                    double dTotalPower = 0;
                    string sql_getStations = string.Format("SELECT distinct StationID,StationName FROM [EnergyAI].[dbo].[Charger_StationInfo] order by StationID");

                    DataTable dtStations = db.Database.SqlQuery(sql_getStations);
                    foreach (DataRow dr in dtStations.Rows)
                    {
                        string? strStationId = dr["StationID"].ToString();
                        string? strStationName = dr["StationName"].ToString();
                        //ChargeStationView

                        decimal dRealPower = 0;
                        string sqlRealPower = string.Format("SELECT [StationID] ,[StationName] ,[实际功率],[总电量] FROM [EnergyAI].[dbo].[ChargeStationView] where StationID='{0}'  ", strStationId);

                        DataTable dtRealPower = db.Database.SqlQuery(sqlRealPower);
                        dRealPower = decimal.Parse(dtRealPower.Rows[0]["实际功率"].ToString()!);
                        if (dRealPower > 0)
                        {
                            decimal dAdviseValue = dRealPower * 1.1M;
                            dPowerLimit = dPowerLimit - dAdviseValue * 0.1M;
                            // 不对外开放-中压充电组
                            if ((strStationName == "不对外开放-中压充电组" || strStationId == "101") && dAdviseValue > 105)
                                break;
                            if ((strStationName == "不对外开放-低压充电组" || strStationId == "102") && dAdviseValue > 163)
                                break;
                            if ((strStationName == "不对外开放-高压充电组" || strStationId == "103") && dAdviseValue > 67)
                                break;
                            if ((strStationName == "不对外开放-低压行政楼东" || strStationId == "446") && dAdviseValue > 137)
                                break;

                            if ((strStationName == "不对外开放-ABB不对外开放-中压充电组（2）" || strStationId == "586") && dAdviseValue > 21)
                                break;
                            //List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                            curChargerOpt.Add(new Power_Opt()
                            {
                                RunMode = 2,
                                //BuildInfoId = "--",//pf.BuildInfoId,
                                // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                                OptTime = DateTime.Parse(strOptTime),
                                RealValue = dRealPower,
                                VarCode = strStationName,
                                OptValue = dAdviseValue,
                                RowStatus = "Y",
                                PowerLimit = dCountryPowerLimit,
                                NextPVPower = dNextPVPower,
                                NextBuildPower = dNextBuildPower,
                                CityPower = dCityPower,
                                CurPVPower = dCurPVPower,
                                AdjustPower = dAdjustPower,
                                PowerPrice = dPowerPrice,
                                Id = Guid.NewGuid()
                            });
                            db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strOptTime, strStationName));
                            int iDelCount = _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                            LogHelper.AddInfo("Power_Opt", $"新增调优数据:【{strStationName}】,调优值:【{dRealPower}】-->【{dAdviseValue}】");
                        }



                    }//end of ChargeStation Adjustion

                }
            }






        }

        static void AdjustAirconditionsByEcnomic(string strOptTime, decimal dPowerPirce, decimal dPowerLimit, decimal dCountryPowerLimit, decimal dNextPVPower, decimal dNextBuildPower, decimal dCityPower, decimal dCurPVPower, decimal dAdjustPower)
        {
            using (var db = _dbHelper.GetDbContext())
            {
                //string strCurDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd");
                //int iCurHour = DateTime.Now.AddHours(1).Hour;
                //string strDeleteDate = strCurDate + ' ' + iCurHour.ToString() + ":00:00";
                //strDeleteDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH:mm");
                string strDeleteDate = strOptTime;
                int iCurHour = DateTime.Parse(strOptTime).Hour;


                string sqlCountryPowerPrice = $"SELECT * from Base_PowerPrice where PriceTime='{iCurHour}' ";
                DataTable dtCountryPowerPrice = db.Database.SqlQuery(sqlCountryPowerPrice);
                decimal dCountryPowerPrice = decimal.Parse(dtCountryPowerPrice.Rows[0]["Price"].ToString()!);
                string strPriceType = dtCountryPowerPrice.Rows[0]["PriceType"].ToString()!;

                decimal AHEU_STA_1 = 1, AHEU_STA_2 = 1, AHEU_STA_3 = 1, CH_STA_1 = 1, CH_STA_2 = 1;
                _curAircondStatus = db.Database.SqlQuery<View_AircondStatus>("SELECT [BuildInfoId],[BuildControlTypeId] ,[WeaVarCode] ,[DecimalValue] FROM [EnergyAI].[dbo].[View_AircondStatus]").ToList();
                //_curAircondStatus = db.Database.SqlQuery<View_AircondStatus>("SELECT   BuildInfoId, BuildControlTypeId, WeaVarCode, DecimalValue FROM      Temp_AircondStatus").ToList();

                foreach (var acs in _curAircondStatus)
                {
                    if (acs.WeaVarCode == "AHEU_STA_1" && acs.DecimalValue == 0)
                    { AHEU_STA_1 = 0; }
                    if (acs.WeaVarCode == "AHEU_STA_2" && acs.DecimalValue == 0)
                    { AHEU_STA_2 = 0; }
                    if (acs.WeaVarCode == "AHEU_STA_3" && acs.DecimalValue == 0)
                    { AHEU_STA_3 = 0; }
                    if (acs.WeaVarCode == "CH-STA_1" && acs.DecimalValue == 0)
                    { CH_STA_1 = 0; }
                    if (acs.WeaVarCode == "CH-STA_2" && acs.DecimalValue == 0)
                    { CH_STA_2 = 0; }

                }
                if (_curAircondStatus.Count > 0)
                {

                    foreach (var acs in _curAircondStatus)
                    {

                        //WeaVarCode DecimalValue
                        //AHEU_Cc_R_1  23.64
                        //AHEU_Cc_R_2  23.33
                        //AHEU_Cc_R_3  23.73
                        //AHEU_STA_1  0.00
                        //AHEU_STA_2  0.00
                        //AHEU_STA_3  0.00
                        //CH_ADMIN_1   20.27
                        //CH_ADMIN_2   18.66
                        //CH - STA_1    0.00
                        //CH - STA_2    0.00
                        //YSJ_DANGQIANFUHE_1  0.00
                        //YSJ_DANGQIANFUHE_2  0.00

                        //string[] airconditions = { "AHEU_Cc_R_1", "AHEU_Cc_R_2", "AHEU_Cc_R_3", "CH_ADMIN_1", "CH_ADMIN_2" };
                        string[] airconditions = { "AHEU_Cc_R_1", "AHEU_Cc_R_2", "AHEU_Cc_R_3", "CH_ADMIN_1", "CH_ADMIN_2" };

                        decimal optValue = 0;
                        if (airconditions.Contains(acs.WeaVarCode))
                        {
                            //if (acs.WeaVarCode == "AHEU_Cc_R_1" && AHEU_STA_1 == 0 ||
                            //      acs.WeaVarCode == "AHEU_Cc_R_2" && AHEU_STA_2 == 0 ||
                            //      acs.WeaVarCode == "AHEU_Cc_R_3" && AHEU_STA_3 == 0 ||
                            //      acs.WeaVarCode == "CH_ADMIN_1" && CH_STA_1 == 0 ||
                            //       acs.WeaVarCode == "CH_ADMIN_2" && CH_STA_2 == 0
                            //      )
                            //    optValue = acs.DecimalValue;
                            //else
                            {
                                //according the outside temperature and periods of day, then set the opt value
                                //1.normal hours of a day
                                Random rd = new Random();
                                //if (iCurHour >= 8 && iCurHour < 10 || iCurHour >= 12 && iCurHour < 15 || iCurHour >= 20 && iCurHour < 22 || iCurHour >= 22)
                                if (strPriceType == "平")
                                {

                                    // if (acs.WeaVarCode == "AHEU_Cc_R_1" || acs.WeaVarCode == "AHEU_Cc_R_2" || acs.WeaVarCode == "AHEU_Cc_R_3" )
                                    if (acs.WeaVarCode == "AHEU_Cc_R_1")
                                        if (AHEU_STA_1 == 0)
                                            optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue - 0.5M;// (decimal)13.0 * rd.Next(90, 100) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_2")
                                        if (AHEU_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue - 0.5M; /// (decimal)13.0 * rd.Next(90, 100) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_3")
                                        if (AHEU_STA_3 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue - 0.5M;// (decimal)13.0 * rd.Next(90, 100) / 100;

                                    // optValue =25.5M;
                                    // if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    if (acs.WeaVarCode == "CH_ADMIN_1")
                                        if (CH_STA_1 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue - 0.5M;// (decimal)10.0 * rd.Next(90, 100) / 100;

                                    if (acs.WeaVarCode == "CH_ADMIN_2")
                                        if (CH_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue - 0.5M; //(decimal)10.0 * rd.Next(90, 100) / 100;

                                }
                                //2.busy hours of a day
                                // if (iCurHour >= 10 && iCurHour < 11 || iCurHour >= 15 && iCurHour < 17 || iCurHour >= 18 && iCurHour < 20 || iCurHour >= 21 && iCurHour < 22)
                                if (strPriceType == "峰")
                                {
                                    //if (acs.WeaVarCode == "AHEU_Cc_R_1" || acs.WeaVarCode == "AHEU_Cc_R_2" || acs.WeaVarCode == "AHEU_Cc_R_3")
                                    //    optValue = (decimal)13.5 * rd.Next(95, 105) / 100;
                                    //if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    //    optValue = (decimal)13.5 * rd.Next(95, 105) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_1")
                                        if (AHEU_STA_1 == 0)
                                            optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 15) / 10;// (decimal)13.5 * rd.Next(95, 105) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_2")
                                        if (AHEU_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 15) / 10;// (decimal)13.5 * rd.Next(95, 105) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_3")
                                        if (AHEU_STA_3 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 15) / 10;// (decimal)13.5 * rd.Next(95, 105) / 100;

                                    // optValue =25.5M;
                                    // if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    if (acs.WeaVarCode == "CH_ADMIN_1")
                                        if (CH_STA_1 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 15) / 10;// (decimal)10.5 * rd.Next(95, 105) / 100;

                                    if (acs.WeaVarCode == "CH_ADMIN_2")
                                        if (CH_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 15) / 10;// (decimal)10.5 * rd.Next(95, 105) / 100;

                                }
                                //3.top busy hours of a day
                                // if (iCurHour == 11 || iCurHour == 17)
                                if (strPriceType == "尖")
                                {
                                    //if (acs.WeaVarCode == "AHEU_Cc_R_1" || acs.WeaVarCode == "AHEU_Cc_R_2" || acs.WeaVarCode == "AHEU_Cc_R_3")
                                    //    optValue = (decimal)14.0 * rd.Next(100, 110) / 100;
                                    //if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    //    optValue = (decimal)13.5 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_1")
                                        if (AHEU_STA_1 == 0)
                                            optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(15, 20) / 10;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_2")
                                        if (AHEU_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 20) / 10;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_3")
                                        if (AHEU_STA_3 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 20) / 10;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    // optValue =25.5M;
                                    // if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    if (acs.WeaVarCode == "CH_ADMIN_1")
                                        if (CH_STA_1 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 20) / 10;// (decimal)11 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "CH_ADMIN_2")
                                        if (CH_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue + rd.Next(10, 20) / 10; //(decimal)11 * rd.Next(100, 110) / 100;

                                }

                                // if (iCurHour == 23 || iCurHour >= 0 && iCurHour < 8)
                                if (strPriceType == "谷")
                                {
                                    //if (acs.WeaVarCode == "AHEU_Cc_R_1" || acs.WeaVarCode == "AHEU_Cc_R_2" || acs.WeaVarCode == "AHEU_Cc_R_3")
                                    //    optValue = (decimal)14.0 * rd.Next(100, 110) / 100;
                                    //if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    //    optValue = (decimal)13.5 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_1")
                                        if (AHEU_STA_1 == 0)
                                            optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_2")
                                        if (AHEU_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "AHEU_Cc_R_3")
                                        if (AHEU_STA_3 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue;// (decimal)14.0 * rd.Next(100, 110) / 100;

                                    // optValue =25.5M;
                                    // if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                                    if (acs.WeaVarCode == "CH_ADMIN_1")
                                        if (CH_STA_1 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue;// (decimal)11 * rd.Next(100, 110) / 100;

                                    if (acs.WeaVarCode == "CH_ADMIN_2")
                                        if (CH_STA_2 == 0) optValue = acs.DecimalValue;
                                        else
                                            optValue = acs.DecimalValue;// (decimal)11 * rd.Next(100, 110) / 100;

                                }
                            }


                            //int countDelete = _dbHelper.DeletePhysicByWhere<Power_Opt>(t => t.OptTime== DateTime.Parse(strDeleteDate));
                            int countDelete = db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate, acs.WeaVarCode));

                            // LogHelper.AddInfo("系统调优", $"删除所有数据共【{countDelete}】条");

                            Guid buildInfoId = (Guid)acs.BuildInfoId;
                            Guid buildControlTypeId = (Guid)acs.BuildControlTypeId;
                            string varCode = acs.WeaVarCode;
                            if (optValue != 0)
                            {
                                List<Power_Opt> curPowerOpt = new List<Power_Opt>();
                                curPowerOpt.Add(new Power_Opt()
                                {
                                    RunMode = 1,
                                    BuildInfoId = buildInfoId,//pf.BuildInfoId,
                                    BuildControlTypeId = buildControlTypeId,//pf.BuildControlTypeId,
                                    OptTime = DateTime.Parse(strDeleteDate),
                                    VarCode = varCode,
                                    RealValue = acs.DecimalValue,
                                    OptValue = optValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    PowerPrice = dPowerPirce,
                                    Id = Guid.NewGuid()
                                });

                                int countAdd1 = _dbHelper.InsertList<Power_Opt>(curPowerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据【{varCode}】输出调优值:【{acs.DecimalValue}】-->【{optValue}】");
                            }
                        }

                    }

                }
            }
        }

        static void AdjustAirconditionsByLimit(string strOptTime, decimal dPowerPirce, decimal dPowerLimit, decimal dCountryPowerLimit, decimal dNextPVPower, decimal dNextBuildPower, decimal dCityPower, decimal dCurPVPower, decimal dAdjustPower)
        {
            using (var db = _dbHelper.GetDbContext())
            {
                //string strCurDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd");
                //int iCurHour = DateTime.Now.AddHours(1).Hour;
                //string strDeleteDate = strCurDate + ' ' + iCurHour.ToString() + ":00:00";
                //strDeleteDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH:mm");
                string strDeleteDate = strOptTime;
                //decimal AHEU_STA_1 = 1, AHEU_STA_2 = 1, AHEU_STA_3 = 1, CH_STA_1 = 1, CH_STA_2 = 1;
                //decimal AHU_Ca_R_1_STA = 1, AHU_Ca_R_2_STA = 1, AHU_Ca_R_3_STA = 1, AHU_Ca_R_4_STA = 1, AHU_Ca_R_5_STA = 1, AHU_Ca_R_6_STA = 1, AHU_Ca_R_7_STA = 1, AHU_Ca_R_8_STA = 1, AHU_Ca_R_9_STA = 1, AHU_Ca_R_10_STA = 1, AHU_Ca_R_11_STA = 1, AHU_Ca_R_12_STA = 1, AHU_Ca_R_13_STA = 1, AHU_Ca_R_14_STA = 1, AHU_Ca_R_15_STA = 1,
                //        AHU_Ca_R_16_STA = 1, AHU_Ca_R_17_STA = 1, AHU_Ca_R_18_STA = 1, AHU_Ca_R_19_STA = 1, AHU_Ca_R_20_STA = 1, AHU_Ca_R_21_STA = 1, AHU_Ca_R_22_STA = 1, AHU_Ca_R_23_STA = 1, AHU_Ca_R_24_STA = 1, AHU_Ca_R_25_STA = 1, AHU_Ca_R_26_STA = 1, AHU_Ca_R_27_STA = 1, AHU_Ca_R_28_STA = 1;

                decimal ITS_CH_2_CH_STA = 1,
  ITS_CH_1_CH_STA = 1,
  //PPMV3405_CH_2_CH_STA = 1,
  //PPMV3405_CH_1_CH_STA = 1,
  AHU_ITS_1_9_STA = 1,
  AHU_ITS_1_8_STA = 1,
  AHU_ITS_1_7_STA = 1,
  AHU_ITS_1_6_STA = 1,
  AHU_ITS_1_5_STA = 1,
  AHU_ITS_1_4_STA = 1,
  AHU_ITS_1_3_STA = 1,
  AHU_ITS_1_2_STA = 1,
  AHU_ITS_1_1_STA = 1;
                _curAircondStatus = db.Database.SqlQuery<View_AircondStatus>("SELECT DISTINCT [BuildInfoId],[BuildControlTypeId] ,[WeaVarCode] ,[DecimalValue] FROM [EnergyAI].[dbo].[View_AircondStatus]").ToList();
                foreach (var acs in _curAircondStatus)
                {
                    //AHU_Ca_R_1_STA 到 AHU_Ca_R_28_STA 
                    if (acs.WeaVarCode == "AHU_ITS_1_1_STA" && acs.DecimalValue == 0) { AHU_ITS_1_1_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_2_STA" && acs.DecimalValue == 0) { AHU_ITS_1_2_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_3_STA" && acs.DecimalValue == 0) { AHU_ITS_1_3_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_4_STA" && acs.DecimalValue == 0) { AHU_ITS_1_4_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_5_STA" && acs.DecimalValue == 0) { AHU_ITS_1_5_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_6_STA" && acs.DecimalValue == 0) { AHU_ITS_1_6_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_7_STA" && acs.DecimalValue == 0) { AHU_ITS_1_7_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_8_STA" && acs.DecimalValue == 0) { AHU_ITS_1_8_STA = 0; }
                    if (acs.WeaVarCode == "AHU_ITS_1_9_STA" && acs.DecimalValue == 0) { AHU_ITS_1_9_STA = 0; }

                    if (acs.WeaVarCode == "ITS_CH_1_CH_STA" && acs.DecimalValue == 0) { ITS_CH_1_CH_STA = 0; }
                    if (acs.WeaVarCode == "ITS_CH_2_CH_STA" && acs.DecimalValue == 0) { ITS_CH_2_CH_STA = 0; }

                    //if (acs.WeaVarCode == "PPMV3405_CH_1_CH_STA" && acs.DecimalValue == 0) { PPMV3405_CH_1_CH_STA = 0; }
                    //if (acs.WeaVarCode == "PPMV3405_CH_2_CH_STA" && acs.DecimalValue == 0) { PPMV3405_CH_2_CH_STA = 0; }

                    //if (acs.WeaVarCode == "AHU_Ca_R_1_Status" && acs.DecimalValue == 0) { AHU_Ca_R_1_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_2_Status" && acs.DecimalValue == 0) { AHU_Ca_R_2_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_3_Status" && acs.DecimalValue == 0) { AHU_Ca_R_3_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_4_Status" && acs.DecimalValue == 0) { AHU_Ca_R_4_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_5_Status" && acs.DecimalValue == 0) { AHU_Ca_R_5_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_6_Status" && acs.DecimalValue == 0) { AHU_Ca_R_6_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_7_Status" && acs.DecimalValue == 0) { AHU_Ca_R_7_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_8_Status" && acs.DecimalValue == 0) { AHU_Ca_R_8_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_9_Status" && acs.DecimalValue == 0) { AHU_Ca_R_9_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_10_Status" && acs.DecimalValue == 0) { AHU_Ca_R_10_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_11_Status" && acs.DecimalValue == 0) { AHU_Ca_R_11_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_12_Status" && acs.DecimalValue == 0) { AHU_Ca_R_12_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_13_Status" && acs.DecimalValue == 0) { AHU_Ca_R_13_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_14_Status" && acs.DecimalValue == 0) { AHU_Ca_R_14_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_15_Status" && acs.DecimalValue == 0) { AHU_Ca_R_15_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_16_Status" && acs.DecimalValue == 0) { AHU_Ca_R_16_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_17_Status" && acs.DecimalValue == 0) { AHU_Ca_R_17_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_18_Status" && acs.DecimalValue == 0) { AHU_Ca_R_18_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_19_Status" && acs.DecimalValue == 0) { AHU_Ca_R_19_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_20_Status" && acs.DecimalValue == 0) { AHU_Ca_R_20_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_21_Status" && acs.DecimalValue == 0) { AHU_Ca_R_21_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_22_Status" && acs.DecimalValue == 0) { AHU_Ca_R_22_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_23_Status" && acs.DecimalValue == 0) { AHU_Ca_R_23_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_24_Status" && acs.DecimalValue == 0) { AHU_Ca_R_24_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_25_Status" && acs.DecimalValue == 0) { AHU_Ca_R_25_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_26_Status" && acs.DecimalValue == 0) { AHU_Ca_R_26_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_27_Status" && acs.DecimalValue == 0) { AHU_Ca_R_27_STA = 0; }
                    //if (acs.WeaVarCode == "AHU_Ca_R_28_Status" && acs.DecimalValue == 0) { AHU_Ca_R_28_STA = 0; }

                    //if (acs.WeaVarCode == "AHEU_STA_2" && acs.DecimalValue == 0)
                    //{ AHEU_STA_2 = 0; }
                    //if (acs.WeaVarCode == "AHEU_STA_3" && acs.DecimalValue == 0)
                    //{ AHEU_STA_3 = 0; }
                    //if (acs.WeaVarCode == "CH-STA_1" && acs.DecimalValue == 0)
                    //{ CH_STA_1 = 0; }
                    //if (acs.WeaVarCode == "CH-STA_2" && acs.DecimalValue == 0)
                    //{ CH_STA_2 = 0; }

                }
                if (_curAircondStatus.Count > 0)
                {

                    foreach (var acs in _curAircondStatus)
                    {

                        //WeaVarCode DecimalValue
                        //AHEU_Cc_R_1  23.64
                        //AHEU_Cc_R_2  23.33
                        //AHEU_Cc_R_3  23.73
                        //AHEU_STA_1  0.00
                        //AHEU_STA_2  0.00
                        //AHEU_STA_3  0.00
                        //CH_ADMIN_1   20.27
                        //CH_ADMIN_2   18.66
                        //CH - STA_1    0.00
                        //CH - STA_2    0.00
                        //YSJ_DANGQIANFUHE_1  0.00
                        //YSJ_DANGQIANFUHE_2  0.00

                        //string[] airconditions = { "AHEU_Cc_R_1", "AHEU_Cc_R_2", "AHEU_Cc_R_3", "CH_ADMIN_1", "CH_ADMIN_2" };

                        string[] airconditions = { "AHU_ITS_1_8_TEMP", "AHU_ITS_1_9_TEMP", "AHU_ITS_1_7_TEMP", "AHU_ITS_1_6_TEMP", "AHU_ITS_1_4_TEMP", "AHU_ITS_1_5_TEMP", "AHU_ITS_1_3_TEMP", "AHU_ITS_1_2_TEMP", "AHU_ITS_1_1_TEMP", "ITS_CH_Ce_1_2_CH_CSWD", "ITS_CH_Ce_1_1_CH_CSWD" };

                        decimal optValue = 0;
                        if (airconditions.Contains(acs.WeaVarCode))
                        {
                            //if (acs.WeaVarCode == "AHEU_Cc_R_1" && AHEU_STA_1 == 0 ||
                            //      acs.WeaVarCode == "AHEU_Cc_R_2" && AHEU_STA_2 == 0 ||
                            //      acs.WeaVarCode == "AHEU_Cc_R_3" && AHEU_STA_3 == 0 ||
                            //      acs.WeaVarCode == "CH_ADMIN_1" && CH_STA_1 == 0 ||
                            //       acs.WeaVarCode == "CH_ADMIN_2" && CH_STA_2 == 0
                            //      )
                            //    optValue = acs.DecimalValue;
                            //else

                            //according the outside temperature and periods of day, then set the opt value
                            //1.normal hours of a day
                            Random rd = new Random();

                            if (acs.WeaVarCode == "AHU_ITS_1_1_TEMP")
                            {
                                if (AHU_ITS_1_1_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_2_TEMP")
                            {
                                if (AHU_ITS_1_2_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_3_TEMP")
                            {
                                if (AHU_ITS_1_3_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_4_TEMP")
                            {
                                if (AHU_ITS_1_4_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_5_TEMP")
                            {
                                if (AHU_ITS_1_5_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_6_TEMP")
                            {
                                if (AHU_ITS_1_6_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_7_TEMP")
                            {
                                if (AHU_ITS_1_7_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_8_TEMP")
                            {
                                if (AHU_ITS_1_8_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "AHU_ITS_1_9_TEMP")
                            {
                                if (AHU_ITS_1_9_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)25.2 && acs.DecimalValue <= 29)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 29 ? 29 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "ITS_CH_Ce_1_1_CH_CSWD")
                            {
                                if (ITS_CH_1_CH_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)7 && acs.DecimalValue <= (decimal)10)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 10 ? 10 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }

                            if (acs.WeaVarCode == "ITS_CH_Ce_1_2_CH_CSWD")
                            {
                                if (ITS_CH_2_CH_STA == 0)
                                { optValue = acs.DecimalValue; }
                                else
                                {
                                    if (acs.DecimalValue >= (decimal)7 && acs.DecimalValue <= (decimal)10)
                                    {
                                        optValue = (acs.DecimalValue + 1) > 10 ? 10 : (acs.DecimalValue + 1);
                                    }
                                    else
                                    {
                                        optValue = acs.DecimalValue;
                                    }
                                }
                            }
                            //if (acs.WeaVarCode == "AHU_Ca_R_1_Temp")
                            //    if (AHU_Ca_R_1_STA == 0)
                            //        optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_2_Temp")
                            //    if (AHU_Ca_R_2_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_3_Temp")
                            //    if (AHU_Ca_R_3_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_4_Temp")
                            //    if (AHU_Ca_R_4_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_5_Temp")
                            //    if (AHU_Ca_R_5_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_6_Temp")
                            //    if (AHU_Ca_R_6_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_7_Temp")
                            //    if (AHU_Ca_R_7_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_8_Temp")
                            //    if (AHU_Ca_R_8_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_9_Temp")
                            //    if (AHU_Ca_R_9_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_10_Temp")
                            //    if (AHU_Ca_R_10_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_11_Temp")
                            //    if (AHU_Ca_R_11_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_12_Temp")
                            //    if (AHU_Ca_R_12_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_13_Temp")
                            //    if (AHU_Ca_R_13_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_14_Temp")
                            //    if (AHU_Ca_R_14_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_15_Temp")
                            //    if (AHU_Ca_R_15_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_16_Temp")
                            //    if (AHU_Ca_R_16_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_17_Temp")
                            //    if (AHU_Ca_R_17_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_18_Temp")
                            //    if (AHU_Ca_R_18_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_19_Temp")
                            //    if (AHU_Ca_R_19_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_20_Temp")
                            //    if (AHU_Ca_R_20_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_21_Temp")
                            //    if (AHU_Ca_R_21_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_22_Temp")
                            //    if (AHU_Ca_R_22_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_23_Temp")
                            //    if (AHU_Ca_R_23_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_24_Temp")
                            //    if (AHU_Ca_R_24_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_25_Temp")
                            //    if (AHU_Ca_R_25_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);


                            //if (acs.WeaVarCode == "AHU_Ca_R_26_Temp")
                            //    if (AHU_Ca_R_26_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_27_Temp")
                            //    if (AHU_Ca_R_27_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHU_Ca_R_28_Temp")
                            //    if (AHU_Ca_R_28_STA == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHEU_Cc_R_1")
                            //    if (AHEU_STA_1 == 0)
                            //        optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHEU_Cc_R_2")
                            //    if (AHEU_STA_2 == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "AHEU_Cc_R_3")
                            //    if (AHEU_STA_3 == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //// optValue =25.5M;
                            //// if (acs.WeaVarCode == "CH_ADMIN_1" || acs.WeaVarCode == "CH_ADMIN_2")
                            //if (acs.WeaVarCode == "CH_ADMIN_1")
                            //    if (CH_STA_1 == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);

                            //if (acs.WeaVarCode == "CH_ADMIN_2")
                            //    if (CH_STA_2 == 0) optValue = acs.DecimalValue;
                            //    else
                            //        optValue = acs.DecimalValue + rd.Next(1, 2);





                            //int countDelete = _dbHelper.DeletePhysicByWhere<Power_Opt>(t => t.OptTime== DateTime.Parse(strDeleteDate));
                            int countDelete = db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate, acs.WeaVarCode));

                            //  LogHelper.AddInfo("系统调优", $"删除所有数据共【{countDelete}】条");

                            Guid buildInfoId = (Guid)acs.BuildInfoId;
                            Guid buildControlTypeId = (Guid)acs.BuildControlTypeId;
                            string varCode = acs.WeaVarCode;
                            if (optValue != 0)
                            {
                                List<Power_Opt> curPowerOpt = new List<Power_Opt>();
                                curPowerOpt.Add(new Power_Opt()
                                {
                                    RunMode = runMode,
                                    BuildInfoId = buildInfoId,//pf.BuildInfoId,
                                    BuildControlTypeId = buildControlTypeId,//pf.BuildControlTypeId,
                                    OptTime = DateTime.Parse(strDeleteDate),
                                    VarCode = varCode,
                                    RealValue = acs.DecimalValue,
                                    OptValue = optValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    PowerPrice = dPowerPirce,
                                    Id = Guid.NewGuid()
                                });

                                int countAdd1 = _dbHelper.InsertList<Power_Opt>(curPowerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据【{varCode}】输出调优值:【{acs.DecimalValue}】-->【{optValue}】");
                            }
                        }

                    }

                }
            }
        }

        static void EcnomicPowerOpt(string strOptTime, decimal dPowerLimit, decimal dCountryPowerLimit, decimal dNextPVPower, decimal dNextBuildPower, decimal dCityPower, decimal dCurPVPower, decimal dAdjustPower)
        {

            using (var db = _dbHelper.GetDbContext())
            {
                //    string strCurDate = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd");
                //int iCurHour = DateTime.Now.AddHours(1).Hour;
                //    string strDeleteDate2 = strCurDate + ' ' + iCurHour.ToString() + ":00:00";
                //    strDeleteDate2=DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH:mm");
                string strDeleteDate2 = strOptTime;
                LogHelper.AddInfo("Power_Opt", $"【调优】预测时间:【每15分钟】-->【{strOptTime}】");
                //delete the history opt data
                db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}'", strOptTime));

                //获取当前小时的电价和光伏电价进行对比
                decimal dPVPowerPrice = 0, dCountryPowerPrice = 0, dPowerPirce = 0;
                string sqlPVPowerPrice = "SELECT * from Base_PowerPrice where PriceTime='-1' ";
                DataTable dtPVPowerPrice = db.Database.SqlQuery(sqlPVPowerPrice);
                dPVPowerPrice = decimal.Parse(dtPVPowerPrice.Rows[0]["Price"].ToString()!);
                //当前小时的电价
                string strCurHour = DateTime.Parse(strOptTime).Hour.ToString();
                string sqlCountryPowerPrice = $"SELECT * from Base_PowerPrice where PriceTime='{strCurHour}' ";
                DataTable dtCountryPowerPrice = db.Database.SqlQuery(sqlCountryPowerPrice);
                dCountryPowerPrice = decimal.Parse(dtCountryPowerPrice.Rows[0]["Price"].ToString()!);

                //下一小时的电价
                string strNextHour = (DateTime.Parse(strOptTime).Hour + 1).ToString();
                string sqlNextCountryPowerPrice = $"SELECT * from Base_PowerPrice where PriceTime='{strNextHour}' ";
                DataTable dtNextCountryPowerPrice = db.Database.SqlQuery(sqlNextCountryPowerPrice);
                decimal dNextCountryPowerPrice = decimal.Parse(dtNextCountryPowerPrice.Rows[0]["Price"].ToString()!);


                //decimal dPriceDiff = dPVPowerPrice - dCountryPowerPrice;
                if (dPVPowerPrice > dCountryPowerPrice) dPowerPirce = dCountryPowerPrice;
                else dPowerPirce = dPVPowerPrice;




                if (dPowerLimit > 0)
                {
                    decimal dAdviseValue = 0;
                    if (dPVPowerPrice < dCountryPowerPrice) //光伏电价小于国网电价
                    {

                        //1.release the ac storage
                        /* [储能]
                           ,[状态]
                           ,[储能当前功率]
                           ,[电池组SOC]
                           ,[储能SOC上限]
                           ,[储能SOC下限]qq
                         */
                        string strACStatus = "";
                        string sqlACStatus = string.Format("SELECT  [储能],[状态],[储能当前功率] ,[电池组SOC] ,[储能SOC上限]  ,[储能SOC下限]  FROM [EnergyAI].[dbo].[ChargeStatus] where 储能='交流仓'");
                        //string sqlACStatus = string.Format(" SELECT '交流仓' [储能],'就绪' [状态], -0.5 [储能当前功率] ,93 [电池组SOC] ,94 [储能SOC上限]  ,12 [储能SOC下限] ");

                        decimal dSavePower = 0;
                        DataTable dtAcStatus = db.Database.SqlQuery(sqlACStatus);
                        if (dtAcStatus.Rows.Count > 0)
                        {
                            strACStatus = dtAcStatus.Rows[0]["状态"].ToString()!;
                            decimal dCurPower = decimal.Parse(dtAcStatus.Rows[0]["储能当前功率"].ToString()!);
                            decimal dSOCVol = decimal.Parse(dtAcStatus.Rows[0]["电池组SOC"].ToString()!);
                            decimal dSOCVolUpper = decimal.Parse(dtAcStatus.Rows[0]["储能SOC上限"].ToString()!);
                            decimal dSOCVolLower = decimal.Parse(dtAcStatus.Rows[0]["储能SOC下限"].ToString()!);

                            if (strACStatus == "充电")
                            {
                                if (dPowerLimit >= 800 && dSOCVol > dSOCVolLower)
                                {
                                    dSavePower = 800 + dCurPower;
                                    dPowerLimit = dPowerLimit - dSavePower;
                                    dAdviseValue = 400;
                                }
                                if (dPowerLimit < 800 && dSOCVol > dSOCVolLower)
                                {
                                    dSavePower = dPowerLimit + dCurPower;
                                    if (dSavePower >= 400)
                                    {
                                        dAdviseValue = 400;
                                        dPowerLimit = dSavePower - 400;
                                    }
                                    else
                                    {
                                        dAdviseValue = dSavePower;
                                        dPowerLimit = 0;
                                    }


                                }

                            }
                            if (strACStatus == "放电")
                            {
                                if (dPowerLimit >= 400 && dSOCVol > dSOCVolLower)
                                {
                                    dSavePower = (400 - dCurPower);
                                    dPowerLimit = dPowerLimit - dSavePower;
                                    dAdviseValue = +dSavePower;
                                }
                                if (dPowerLimit < 400 && dSOCVol > dSOCVolLower)
                                {
                                    dSavePower = dPowerLimit; ;
                                    dPowerLimit = 0;
                                    dAdviseValue = +dSavePower;
                                }

                            }

                            if (strACStatus == "就绪" && dSOCVol > dSOCVolLower)
                            {
                                if (dPowerLimit >= 400)
                                {
                                    dSavePower = 400;
                                    dPowerLimit = dPowerLimit - dSavePower;
                                    dAdviseValue = +dSavePower;
                                }
                                else
                                {
                                    dSavePower = dPowerLimit;
                                    dPowerLimit = dPowerLimit - dSavePower;
                                    dAdviseValue = +dSavePower;
                                }

                            }
                            //if (strACStatus == "放电")
                            //{
                            //    dPowerLimit = dPowerLimit - 800;

                            //}

                            if (strACStatus == "充电" || strACStatus == "就绪" || strACStatus == "放电")
                            {

                                List<Power_Opt> curACPowerOpt = new List<Power_Opt>();
                                curACPowerOpt.Add(new Power_Opt()
                                {
                                    RunMode = 1,
                                    //BuildInfoId = "--",//pf.BuildInfoId,
                                    //BuildControlTypeId = "--",//pf.BuildControlTypeId,DateTime.Now.ToString("yyyy-MM-dd HH:00:00")
                                    OptTime = DateTime.Parse(strDeleteDate2),
                                    VarCode = "交流仓",
                                    RealValue = dCurPower,
                                    OptValue = dAdviseValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    PowerPrice = dPowerPirce,
                                    Remark = string.Format("Status:{0},Vol:{1},Upper:{2},Lower:{3}", strACStatus, dSOCVol, dSOCVolUpper, dSOCVolLower),
                                    Id = Guid.NewGuid()
                                });

                                int iDelCount = _dbHelper.InsertList<Power_Opt>(curACPowerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据:【交流仓】,调优值:【{dCurPower}】-->【{dAdviseValue}】");

                            }
                        }
                        //2.release the dc storage
                        dAdviseValue = 0;
                        if (dPowerLimit > 0)
                        {
                            string strDCStatus = "";
                            string sqlDCStatus = string.Format("SELECT  [储能],[状态],[储能当前功率] ,[电池组SOC] ,[储能SOC上限]  ,[储能SOC下限]  FROM [EnergyAI].[dbo].[ChargeStatus] where 储能='直流仓'");
                            //string sqlDCStatus = string.Format(" SELECT '直流仓' [储能],'就绪' [状态], 0 [储能当前功率] ,91 [电池组SOC] ,92 [储能SOC上限]  ,20 [储能SOC下限] ");

                            DataTable dtDCStatus = db.Database.SqlQuery(sqlDCStatus);
                            if (dtDCStatus.Rows.Count > 0)
                            {
                                strDCStatus = dtDCStatus.Rows[0]["状态"].ToString()!;
                                decimal dDCCurPower = decimal.Parse(dtDCStatus.Rows[0]["储能当前功率"].ToString()!);
                                decimal dSOCVol = decimal.Parse(dtDCStatus.Rows[0]["电池组SOC"].ToString()!);
                                decimal dSOCVolUpper = decimal.Parse(dtDCStatus.Rows[0]["储能SOC上限"].ToString()!);
                                decimal dSOCVolLower = decimal.Parse(dtDCStatus.Rows[0]["储能SOC下限"].ToString()!);

                                //if (strDCStatus == "充电")
                                //{
                                //    if (dPowerLimit >= 400 && dSOCVol > dSOCVolLower)
                                //    {
                                //        dSavePower = 400 - dDCCurPower;
                                //        dPowerLimit = dPowerLimit - dSavePower;
                                //        dAdviseValue = 400 / 2;
                                //    }
                                //    if (dPowerLimit < 400 && dSOCVol > dSOCVolLower)
                                //    {
                                //        dSavePower = (dPowerLimit);
                                //        dPowerLimit = 0;
                                //        dAdviseValue = (dPowerLimit + dDCCurPower); ;
                                //    }
                                //}

                                if (strDCStatus == "充电")
                                {
                                    if (dPowerLimit >= 400 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = 400 + dDCCurPower;
                                        dPowerLimit = dPowerLimit - dSavePower;
                                        dAdviseValue = 200;
                                    }
                                    else
                                    if (dPowerLimit < 400 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = dPowerLimit + dDCCurPower;
                                        if (dSavePower >= 200)
                                        {
                                            dAdviseValue = 200;
                                            dPowerLimit = dSavePower - 200;
                                        }
                                        else
                                        {
                                            dAdviseValue = dSavePower;
                                            dPowerLimit = 0;
                                        }


                                    }

                                }

                                if (strDCStatus == "放电")
                                {
                                    if (dPowerLimit >= 200 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = (200 - dDCCurPower);
                                        dPowerLimit = dPowerLimit - dSavePower;
                                        dAdviseValue = dSavePower;
                                    }
                                    else
                                    if (dPowerLimit < 200 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = (200 - dDCCurPower); ;
                                        dPowerLimit = 0;
                                        dAdviseValue = +dSavePower;
                                    }

                                }
                                if (strDCStatus == "就绪")
                                {
                                    if (dPowerLimit >= 200 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = 200;
                                        dPowerLimit = dPowerLimit - dSavePower;
                                        dAdviseValue = +dSavePower;
                                    }
                                    else
                                    if (dPowerLimit < 200 && dSOCVol > dSOCVolLower)
                                    {
                                        dSavePower = (dPowerLimit - dDCCurPower);
                                        dPowerLimit = 0;
                                        dAdviseValue = dSavePower;
                                    }

                                }
                                db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, "直流仓"));
                                // if (strDCStatus == "充电" || strDCStatus == "就绪" || strDCStatus == "放电")
                                {

                                    List<Power_Opt> curDCPowerOpt = new List<Power_Opt>();
                                    curDCPowerOpt.Add(new Power_Opt()
                                    {
                                        RunMode = 1,
                                        //BuildInfoId = "--",//pf.BuildInfoId,
                                        //BuildControlTypeId = "--",//pf.BuildControlTypeId,
                                        OptTime = DateTime.Parse(strDeleteDate2),
                                        VarCode = "直流仓",
                                        RealValue = dDCCurPower,
                                        OptValue = dAdviseValue,
                                        RowStatus = "Y",
                                        PowerLimit = dCountryPowerLimit,
                                        NextPVPower = dNextPVPower,
                                        NextBuildPower = dNextBuildPower,
                                        CityPower = dCityPower,
                                        CurPVPower = dCurPVPower,
                                        AdjustPower = dAdjustPower,
                                        PowerPrice = dPowerPirce,
                                        Remark = string.Format("Status:{0},Vol:{1},Upper:{2},Lower:{3}", strDCStatus, dSOCVol, dSOCVolUpper, dSOCVolLower),
                                        Id = Guid.NewGuid()
                                    });

                                    int iDelCount = _dbHelper.InsertList<Power_Opt>(curDCPowerOpt, null);
                                    LogHelper.AddInfo("Power_Opt", $"新增调优数据:【直流仓】,调优值:【{dDCCurPower}】-->【{dAdviseValue}】");
                                }
                            }

                        }
                    }
                    //3.release the  charger
                    //string sql_getStations = string.Format("SELECT distinct StationID,StationName FROM [EnergyAI].[dbo].[Charger_StationInfo] order by StationID");

                    //DataTable dtStations = db.Database.SqlQuery(sql_getStations);
                    //foreach (DataRow dr in dtStations.Rows)
                    //{
                    //    string? strStationId = dr["StationID"].ToString();
                    //    string? strStationName = dr["StationName"].ToString();
                    //    db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, strStationName));
                    //}
                    if (dPowerLimit > 0) // 充电桩调节
                    {
                        // 新增逻辑当下一个小时的电费大于当前电费的情况下并且小于光伏电费的情况下，也要调整充电桩功率增加10%
                        //否则就进行功率下调10%
                        double dTotalPower = 0;
                        string sql_getStations = string.Format("SELECT distinct StationID,StationName FROM [EnergyAI].[dbo].[Charger_StationInfo] order by StationID");

                        DataTable dtStations = db.Database.SqlQuery(sql_getStations);
                        foreach (DataRow dr in dtStations.Rows)
                        {
                            string? strStationId = dr["StationID"].ToString();
                            string? strStationName = dr["StationName"].ToString();
                            //ChargeStationView

                            decimal dRealPower = 0;
                            string sqlRealPower = string.Format("SELECT [StationID] ,[StationName] ,[实际功率],[总电量] FROM [EnergyAI].[dbo].[ChargeStationView] where StationID='{0}'  ", strStationId);

                            DataTable dtRealPower = db.Database.SqlQuery(sqlRealPower);
                            dRealPower = decimal.Parse(dtRealPower.Rows[0]["实际功率"].ToString()!);
                            if (dRealPower > 0)
                            {
                                if (dCountryPowerPrice < dNextCountryPowerPrice && dCountryPowerPrice < dPVPowerPrice)
                                {
                                    dAdviseValue = dRealPower * 1.1M;
                                    dPowerLimit = dPowerLimit - dAdviseValue * 0.1M;

                                    if (strStationName == "不对外开放-低压充电组" && dAdviseValue > 163)
                                        break;
                                    if (strStationName == "不对外开放-高压充电组" && dAdviseValue > 67)
                                        break;
                                    if (strStationName == "不对外开放-低压行政楼东" && dAdviseValue > 137)
                                        break;
                                    if (strStationName == "不对外开放-中压充电组" && dAdviseValue > 105)
                                        break;
                                    if (strStationName == "不对外开放-ABB不对外开放-中压充电组（2）" && dAdviseValue > 21)
                                        break;
                                }
                                else
                                {
                                    dAdviseValue = dRealPower * 0.9M;
                                    dPowerLimit = dPowerLimit - dAdviseValue * 0.1M;
                                    if (strStationName == "不对外开放-低压充电组" && dAdviseValue < 100)
                                        break;
                                    if (strStationName == "不对外开放-高压充电组" && dAdviseValue < 40)
                                        break;
                                    if (strStationName == "不对外开放-低压行政楼东" && dAdviseValue < 80)
                                        break;
                                    if (strStationName == "不对外开放-中压充电组" && dAdviseValue < 65)
                                        break;
                                    if (strStationName == "不对外开放-ABB不对外开放-中压充电组（2）" && dAdviseValue < 12)
                                        break;
                                }
                                List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                                curChargerOpt.Add(new Power_Opt()
                                {
                                    RunMode = 1,
                                    //BuildInfoId = "--",//pf.BuildInfoId,
                                    // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                                    OptTime = DateTime.Parse(strDeleteDate2),
                                    RealValue = dRealPower,
                                    VarCode = strStationName,
                                    OptValue = dAdviseValue,
                                    RowStatus = "Y",
                                    PowerLimit = dCountryPowerLimit,
                                    NextPVPower = dNextPVPower,
                                    NextBuildPower = dNextBuildPower,
                                    CityPower = dCityPower,
                                    CurPVPower = dCurPVPower,
                                    AdjustPower = dAdjustPower,
                                    PowerPrice = dPowerPirce,
                                    Id = Guid.NewGuid()
                                });
                                //db.Database.ExecuteSqlCommand(string.Format("delete from Power_Opt where OptTime='{0}' and VarCode='{1}'", strDeleteDate2, strStationName));
                                int iDelCount = _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                                LogHelper.AddInfo("Power_Opt", $"新增调优数据:【{strStationName}】,调优值:【{dRealPower}】-->【{dAdviseValue}】");
                            }

                        }

                    }//end of 充电桩 Adjustion

                    if (dPowerLimit > 0) //Adjust the airconditions
                    {
                        AdjustAirconditionsByEcnomic(strOptTime, dPowerPirce, dAdjustPower, dCountryPowerLimit, dNextPVPower, dNextBuildPower, dCityPower, dCurPVPower, dAdjustPower);
                    }

                }
                else //电量有多余，进行余电上网
                {
                    //获取当前电价
                    decimal dPowerPrice = 0;
                    string sqlPowerPrice = "SELECT * from Base_PowerPrice where PriceTime='-2' ";
                    DataTable dtPowerPrice = db.Database.SqlQuery(sqlPowerPrice);
                    dPowerPrice = decimal.Parse(dtPowerPrice.Rows[0]["Price"].ToString()!);


                    List<Power_Opt> curChargerOpt = new List<Power_Opt>();
                    curChargerOpt.Add(new Power_Opt()
                    {
                        RunMode = 2,
                        //BuildInfoId = "--",//pf.BuildInfoId,
                        // BuildControlTypeId = strStationId,//pf.BuildControlTypeId,
                        OptTime = DateTime.Parse(strDeleteDate2),
                        RealValue = 0,
                        VarCode = "余电上网",
                        OptValue = 0,
                        RowStatus = "Y",
                        PowerLimit = dCountryPowerLimit,
                        NextPVPower = dNextPVPower,
                        NextBuildPower = dNextBuildPower,
                        CityPower = dCityPower,
                        CurPVPower = dCurPVPower,
                        AdjustPower = dAdjustPower,
                        PowerPrice = dPowerPrice,
                        Id = Guid.NewGuid()
                    });
                    _dbHelper.InsertList<Power_Opt>(curChargerOpt, null);
                    LogHelper.AddInfo("Power_Opt", $"新增调优数据:【余电上网】,调优值:【{dAdjustPower}】");
                }


            }
        }
        static void DataInit()
        {
            if (int.TryParse(Untils.GetAppSetting("AppConfig.OptCalcRealtimeCycle"), out int temp))
            {
                loopForecastMs = temp * 1000;
            }
            if (int.TryParse(Untils.GetAppSetting("AppConfig.RunMode"), out int modeTemp))
            {
                runMode = modeTemp;
            }

#if DEBUG
            //loopRealTimeMs = 60000;

#endif
            //_allPhotoVoltaics = _dbHelper.GetList<Wea_PhotoVoltaics>();
            //LogHelper.AddInfo("DataInit", $"获取光伏板列表【{_allPhotoVoltaics.Count}】个");
            // _allBuildInfos = _dbHelper.GetList<Build_BuildInfo>();
            // LogHelper.AddInfo("DataInit", $"获取所有楼宇列表【{string.Join(",", _allBuildInfos.Select(_ => _.BuildName).Distinct().ToList())}】");


            // _allBuildControlTypes = _dbHelper.GetList<Build_BuildControlType>();
            //  LogHelper.AddInfo("DataInit", $"获取所有楼宇控制分类列表【{string.Join(",", _allBuildControlTypes.Select(_ => _.TypeName).Distinct().ToList())}】");
        }

        /// <summary>
        /// 获取标识
        /// </summary>
        /// <param name="park">园区</param>
        /// <returns></returns>
        static string GetParkFlag(Base_ParkInfo park, string prefix)
        {
            return prefix + "_" + park.ParkName;
        }

        //和计算有关的天气编码列表
        public static readonly List<string> WeaVarCodes = new List<string>()
        { "Temperature", "Humidity" };

        /// <summary>
        /// 获取预报天气数据_楼宇预测
        /// </summary>
        /// <returns></returns>
        static List<Wea_ForecastWeather> GetForecastWeatherList(Base_ParkInfo park, DateTime now)
        {
            using (var db = _dbHelper.GetDbContext())
            {
                //当前园区所有楼宇
                var curBuilds = _allBuildInfos.Where(_ => _.ParkInfoId == park.Id).Select(_ => _.Id).Distinct().ToList();
                //DateTime? lastTime = _dbHelper.GetQuery<Build_CalcForecast, Guid>(db, _ => _.BuildInfoId != null && curBuilds.Contains(_.BuildInfoId.Value)).Select(_ => _.WeaTime).Max();
                var query = _dbHelper.GetQuery<Wea_ForecastWeather, Guid>(db, _ => _.ParkInfoId == park.Id && WeaVarCodes.Contains(_.WeaVarCode) && _.WeaTime >= now);
                //if (lastTime.HasValue)
                //{
                //    query = query.Where(_ => _.WeaTime > lastTime);
                //}
                var list = query.ToList();
                return list;
            }

        }

        //如有ZEE600的值，取ZEE600  key为ZEE600的WeaVarCode  value为对应气象数据WeaVarCode
        static readonly Dictionary<string, string> zee600Code = new Dictionary<string, string>()
        {
            {"ZENON_Temperature","Temperature" },
            {"ZENON_Humidity","Humidity" }
        };

        /// <summary>
        /// 获取实时天气数据_楼宇预测
        /// </summary>
        /// <returns></returns>
        static List<Wea_RealTimeWeather> GetRealTimeWeatherList(Base_ParkInfo park, out List<Build_CalcRealTime> aboutZenonList)
        {
            aboutZenonList = new List<Build_CalcRealTime>();
            List<string> weaCodes = new List<string>() { "Temperature", "Humidity" };
            using (var db = _dbHelper.GetDbContext())
            {
                //当前园区所有楼宇
                var curBuilds = _allBuildInfos.Where(_ => _.ParkInfoId == park.Id).Select(_ => _.Id).Distinct().ToList();
                DateTime? lastTime = _dbHelper.GetQuery<Build_CalcRealTime, Guid>(db, _ => _.BuildInfoId != null && curBuilds.Contains(_.BuildInfoId.Value) && _.WeaVarCode == WEA_VAR_CODE).Select(_ => _.WeaTime).Max();
                var query = _dbHelper.GetQuery<Wea_RealTimeWeather, Guid>(db, _ => _.ParkInfoId == park.Id && weaCodes.Contains(_.WeaVarCode));
                if (lastTime.HasValue)
                {
                    query = query.Where(_ => _.WeaTime > lastTime);
                }
                else
                {
                    query = query.OrderByDescending(_ => _.WeaTime).Take(200);
                }
                var list = query.ToList();
                if (list == null || list.Count == 0) return new List<Wea_RealTimeWeather>();
                //第一次计算时，取最近的1个时间点进行计算
                if (lastTime.HasValue == false)
                {
                    DateTime? maxWeaTime = list.Select(_ => _.WeaTime).Max();
                    if (maxWeaTime.HasValue)
                    {
                        list = list.Where(_ => _.WeaTime == maxWeaTime).ToList();
                    }
                }

                //天气数据所有时间点
                List<DateTime?> weaTimes = list.Select(_ => _.WeaTime).Distinct().ToList();

                aboutZenonList = _dbHelper.GetQuery<Build_CalcRealTime, Guid>(db, _ => _.BuildInfoId != null && curBuilds.Contains(_.BuildInfoId.Value) && zee600Code.Keys.Contains(_.WeaVarCode) && weaTimes.Contains(_.WeaTime)).ToList();
                return list;
            }

        }

        static void Calculate_ForecastWeather(Base_ParkInfo park)
        {
            try
            {
                if (_allBuildInfos.Count == 0)
                {
                    throw new Exception("楼宇数据为空");
                }
                if (_allBuildControlTypes.Count == 0)
                {
                    throw new Exception("楼宇控制分类数据为空");
                }
                List<Build_CalcForecast> build_ForecastWeas = new List<Build_CalcForecast>();
                //获取所有未来的预报天气数据
                DateTime now = DateTime.Now;
                now = now.Date.AddHours(now.Hour).AddMinutes(now.Minute);
                var forecastWeas = GetForecastWeatherList(park, now);
                LogHelper.AddInfo(GetParkFlag(park, "预报天气"), $"获取天气数据共【{forecastWeas.Count}】条{(forecastWeas.Count > 0 ? "最小天气时间【" + forecastWeas.Select(_ => _.WeaTime).Min()?.ToString("yyyy-MM-dd HH:mm:ss") + "】" : "")}");
                //当前所有时间点是否为节假日 key为天气时间+楼宇控制分类ID
                List<DateTime> allWeaTimeDate = forecastWeas.Where(_ => _.WeaTime.HasValue).Select(_ => _.WeaTime.Value.Date).Distinct().ToList();
                Dictionary<string, bool> weaTimeDateIsHolidayDic = new Dictionary<string, bool>();
                foreach (var one in allWeaTimeDate)
                {
                    foreach (var con in _allBuildControlTypes)
                    {
                        string key = one.Date.ToString("yyyy-MM-dd") + "_" + con.Id.ToString();
                        bool val = IsHoliday("预报天气", park, one.Date, con.Id);
                        if (weaTimeDateIsHolidayDic.ContainsKey(key))
                        {
                            if (weaTimeDateIsHolidayDic[key] != val)
                            {
                                LogHelper.AddWarn(GetParkFlag(park, "预报天气"), $"时间_楼宇控制分类主键【{key}】有多个不同的节假日状态");
                            }
                        }
                        else
                        {
                            weaTimeDateIsHolidayDic.Add(key, val);
                        }
                    }
                }
                //楼宇ID+控制分类ID+时间
                List<string> addedTimes = new List<string>();
                foreach (var one in forecastWeas)
                {
                    if (one.WeaTime == null) continue;
                    var curBuilds = _allBuildInfos.Where(_ => _.ParkInfoId == park.Id).ToList();
                    foreach (var build in curBuilds)
                    {
                        foreach (var con in _allBuildControlTypes)
                        {
                            build_ForecastWeas.Add(new Build_CalcForecast()
                            {
                                BuildInfoId = build.Id,
                                BuildControlTypeId = con.Id,
                                CreationTime = DateTime.Now,
                                DecimalValue = one.DecimalValue,
                                Id = Guid.NewGuid(),
                                IsTxtOfValue = one.IsTxtOfValue,
                                RowStatus = "Y",
                                TxtValue = one.TxtValue,
                                WeaTime = one.WeaTime,
                                WeaVarCode = one.WeaVarCode,
                                WebLastUpdateTime = one.WebLastUpdateTime
                            });
                            if (one.WeaVarCode == "Temperature" && one.DecimalValue.HasValue && one.WeaTime.HasValue)
                            {
                                string addTime = build.Id.ToString() + con.Id.ToString() + one.WeaTime.Value.ToString();
                                if (addedTimes.Contains(addTime) == false)
                                {
                                    string key = one.WeaTime.Value.Date.ToString("yyyy-MM-dd") + "_" + con.Id.ToString();
                                    build_ForecastWeas.Add(new Build_CalcForecast()
                                    {
                                        BuildInfoId = build.Id,
                                        BuildControlTypeId = con.Id,
                                        CreationTime = DateTime.Now,
                                        DecimalValue = weaTimeDateIsHolidayDic[key] ? 1 : 0,
                                        Id = Guid.NewGuid(),
                                        IsTxtOfValue = false,
                                        RowStatus = "Y",
                                        TxtValue = null,
                                        WeaTime = one.WeaTime,
                                        WeaVarCode = WEA_VAR_CODE,
                                        WebLastUpdateTime = DateTime.Now
                                    });
                                    addedTimes.Add(addTime);
                                }
                            }
                        }
                    }
                }
                //添加AI
                var gy = build_ForecastWeas.GroupBy(_ => new { _.BuildInfoId, _.BuildControlTypeId });
                List<Build_CalcForecast> aiAdds = new List<Build_CalcForecast>();
                foreach (var one in gy)
                {
                    try
                    {
                        aiAdds.AddRange(BuildCalculateAIDbHelper.GetAIListOfBuildCalcForecast(one.Key.BuildInfoId, one.Key.BuildControlTypeId, one.ToList(), out string msg));
                        if (string.IsNullOrWhiteSpace(msg) == false)
                        {
                            LogHelper.AddWarn(GetParkFlag(park, "预报天气"), msg);
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        LogHelper.AddErr(GetParkFlag(park, "预报天气"), ex);
                    }
                }
                build_ForecastWeas.AddRange(aiAdds);

                //删除所有数据
                int count = _dbHelper.DeletePhysicByWhere<Build_CalcForecast>(_ => _.RowStatus == "Y");
                LogHelper.AddInfo(GetParkFlag(park, "预报天气"), $"删除所有数据共【{count}】条");
                //保存预报天气的楼宇预测
                count = _dbHelper.InsertList<Build_CalcForecast>(build_ForecastWeas, null);
                LogHelper.AddInfo(GetParkFlag(park, "预报天气"), $"新增数据共【{count}】条");
            }
            catch (Exception ex)
            {
                LogHelper.AddErr(GetParkFlag(park, "预报天气"), ex);
            }
        }

        static void Calculate_RealTimeWeather(Base_ParkInfo park)
        {
            try
            {
                if (_allBuildInfos.Count == 0)
                {
                    throw new Exception("楼宇数据为空");
                }

                if (_allBuildControlTypes.Count == 0)
                {
                    throw new Exception("楼宇控制分类数据为空");
                }

                List<Build_CalcRealTime> build_RealTimeWeas = new List<Build_CalcRealTime>();
                //获取所有未来的预报天气数据
                var realTimeWeas = GetRealTimeWeatherList(park, out List<Build_CalcRealTime> valsOfZENON);
                LogHelper.AddInfo(GetParkFlag(park, "实时天气"), $"获取天气数据共【{realTimeWeas.Count}】条{(realTimeWeas.Count > 0 ? "天气时间【" + realTimeWeas[0].WeaTime?.ToString("yyyy-MM-dd HH:mm:ss") + "】" : "")}");
                //当前所有时间点是否为节假日
                List<DateTime> allWeaTimeDate = realTimeWeas.Where(_ => _.WeaTime.HasValue).Select(_ => _.WeaTime.Value.Date).Distinct().ToList();
                //Dictionary<DateTime, bool> weaTimeDateIsHolidayDic = new Dictionary<DateTime, bool>();
                //foreach (var one in allWeaTimeDate)
                //{
                //    weaTimeDateIsHolidayDic.Add(one, IsHoliday("实时天气", park, one));
                //}
                Dictionary<string, bool> weaTimeDateIsHolidayDic = new Dictionary<string, bool>();
                foreach (var one in allWeaTimeDate)
                {
                    foreach (var con in _allBuildControlTypes)
                    {
                        string key = one.Date.ToString("yyyy-MM-dd") + "_" + con.Id.ToString();
                        bool val = IsHoliday("预报天气", park, one.Date, con.Id);
                        if (weaTimeDateIsHolidayDic.ContainsKey(key))
                        {
                            if (weaTimeDateIsHolidayDic[key] != val)
                            {
                                LogHelper.AddWarn(GetParkFlag(park, "实时天气"), $"时间_楼宇控制分类主键【{key}】有多个不同的节假日状态");
                            }
                        }
                        else
                        {
                            weaTimeDateIsHolidayDic.Add(key, val);
                        }
                    }
                }

                //楼宇ID+控制分类ID+时间
                List<string> addedTimes = new List<string>();
                foreach (var one in realTimeWeas)
                {
                    if (one.WeaTime == null) continue;
                    var curBuilds = _allBuildInfos.Where(_ => _.ParkInfoId == park.Id).ToList();
                    foreach (var build in curBuilds)
                    {
                        foreach (var con in _allBuildControlTypes)
                        {
                            //ZENON的值，暂时不需要处理事务，因为只计算节假日不需要WeaVarCode对应的值
                            build_RealTimeWeas.Add(new Build_CalcRealTime()
                            {
                                BuildInfoId = build.Id,
                                BuildControlTypeId = con.Id,
                                CreationTime = DateTime.Now,
                                DecimalValue = one.DecimalValue,
                                Id = Guid.NewGuid(),
                                IsTxtOfValue = one.IsTxtOfValue,
                                RowStatus = "Y",
                                TxtValue = one.TxtValue,
                                WeaTime = one.WeaTime,
                                WeaVarCode = one.WeaVarCode,
                                WebLastUpdateTime = one.WebLastUpdateTime
                            });
                            if (one.WeaVarCode == "Temperature" && one.DecimalValue.HasValue && one.WeaTime.HasValue)
                            {
                                string addTime = build.Id.ToString() + con.Id.ToString() + one.WeaTime.Value.ToString();
                                if (addedTimes.Contains(addTime) == false)
                                {
                                    string key = one.WeaTime.Value.ToString("yyyy-MM-dd") + "_" + con.Id.ToString();
                                    build_RealTimeWeas.Add(new Build_CalcRealTime()
                                    {
                                        BuildInfoId = build.Id,
                                        BuildControlTypeId = con.Id,
                                        CreationTime = DateTime.Now,
                                        DecimalValue = weaTimeDateIsHolidayDic[key] ? 1 : 0,
                                        Id = Guid.NewGuid(),
                                        IsTxtOfValue = false,
                                        RowStatus = "Y",
                                        TxtValue = null,
                                        WeaTime = one.WeaTime,
                                        WeaVarCode = WEA_VAR_CODE,
                                        WebLastUpdateTime = DateTime.Now
                                    });
                                    addedTimes.Add(addTime);
                                }
                            }
                        }
                    }
                }

                //保存预报天气的楼宇预测
                int count = _dbHelper.InsertList<Build_CalcRealTime>(build_RealTimeWeas, null);
                LogHelper.AddInfo(GetParkFlag(park, "实时天气"), $"新增数据共【{count}】条");
            }
            catch (Exception ex)
            {
                LogHelper.AddErr(GetParkFlag(park, "实时天气"), ex);
            }
        }

        static bool IsHoliday(string prefix, Base_ParkInfo park, DateTime time, Guid BuildControlTypeId)
        {
            var count = _dbHelper.GetCount<Build_Holiday>(_ => _.CurDate.HasValue ? _.CurDate.Value.Date == time.Date : false && (_.BuildControlTypeId == BuildControlTypeId || _.BuildControlTypeId.HasValue == false));
            if (count > 0)
            {
                return true;
            }
            else
            {
                if (time.DayOfWeek == DayOfWeek.Saturday || time.DayOfWeek == DayOfWeek.Sunday)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

    }
}
