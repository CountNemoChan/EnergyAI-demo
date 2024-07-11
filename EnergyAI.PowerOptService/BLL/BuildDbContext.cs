using EnergyAi.Base;
using EnergyAi.Build;
using EnergyAi.Notice;
using EnergyAi.WEA;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EnergyAI.PowerOptService.BLL
{
    public class BuildDbContext : DbContext
    {
        public DbSet<Wea_ForecastWeather> Wea_ForecastWeather { get; set; }
        public DbSet<Wea_RealTimeWeather> Wea_RealTimeWeather { get; set; }

        public DbSet<Base_ParkInfo> Base_ParkInfo { get; set; }
        public DbSet<Notice_AlarmNoticeConfig> Notice_AlarmNoticeConfig { get; set; }
        public DbSet<Notice_AlarmRecord> Notice_AlarmRecord { get; set; }
        public DbSet<Build_BuildControlType> Build_BuildControlType { get; set; }
        public DbSet<Build_BuildInfo> Build_BuildInfo { get; set; }
        public DbSet<Build_BuildZEE600VarMap> Build_BuildZEE600VarMap { get; set; }
        public DbSet<Build_Holiday> Build_Holiday { get; set; }
        public DbSet<Wea_PhotoVoltaics> Wea_PhotoVoltaics { get; set; }
        public DbSet<Wea_ZEE600VarMap> Wea_ZEE600VarMap { get; set; }

        public DbSet<Build_CalcForecast> Build_CalcForecast { get; set; }
        public DbSet<View_PowerForecast> View_PowerForecast { get; set; }
        public DbSet<View_AircondStatus> View_AircondStatus { get; set; }
        public DbSet<Build_CalcRealTime> Build_CalcRealTime { get; set; }
        public DbSet<Wea_CalcPVForecast> Wea_CalcPVForecast { get; set; }
        public DbSet<Wea_CalcPVRealTime> Wea_CalcPVRealTime { get; set; }

        /// <summary>
        /// DbContext
        /// </summary>
        /// <param name="options"></param>
        public BuildDbContext(DbContextOptions<BuildDbContext> options)
        : base(options)
        {
            //设置执行命令的超时时间限制，！！！重要！！！
            this.Database.SetCommandTimeout(300);
        }

        static string _connstr = "";
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public static string SqlConnectionString
        {
            get
            {
                if (string.IsNullOrWhiteSpace(_connstr))
                {
                    var fileName = "appsettings.json";
                    var directory = AppContext.BaseDirectory;
                    directory = directory.Replace("\\", "/");

                    var filePath = $"{directory}/{fileName}";
                    if (!File.Exists(filePath))
                    {
                        var length = directory.IndexOf("/bin");
                        filePath = $"{directory.Substring(0, length)}/{fileName}";
                    }

                    var builder = new ConfigurationBuilder()
                        .AddJsonFile(filePath, false, true);

                    var _configuration = builder.Build();
                    _connstr = _configuration.GetConnectionString("Default");
                    _connstr = CommonUntils.Untils.EtpDecrypt(_connstr);
                }
                return _connstr;
            }
        }

        /// <summary>
        /// 静态获取DbContext，由调用方式Dispose
        /// </summary>
        /// <returns>DbContext</returns>
        public static BuildDbContext GetDbContext()
        {
            var builder = new DbContextOptionsBuilder<BuildDbContext>();
            builder.UseSqlServer(SqlConnectionString);
            var dbContext = new BuildDbContext(builder.Options);
            return dbContext;
        }
    }
}
