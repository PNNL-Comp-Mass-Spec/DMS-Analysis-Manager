using System;
using System.Collections.Generic;
using AnalysisManagerBase;

namespace TestAScorePlugIn {

    class StatusFileStub : IStatusFile {
        #region IStatusFile Members

        public string RemoteMgrName { get; set; }

        public int CpuUtilization {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string CurrentOperation {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string Dataset {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public float BrokerDBUpdateIntervalMinutes { get; }

        public string FileNamePath {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public int JobNumber {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public int JobStep {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public bool LogToMsgQueue {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string MessageQueueTopic {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string MessageQueueURI {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string MgrName {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public EnumMgrStatus MgrStatus {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string MostRecentJobInfo {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public int ProgRunnerProcessID { get; set; }
        public float ProgRunnerCoreUsage { get; set; }

        public DateTime TaskStartTime { get; set; }

        public float Progress {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public int SpectrumCount {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public EnumTaskStatus TaskStatus {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public EnumTaskStatusDetail TaskStatusDetail {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public string Tool {
            get {
                return "Stub";
            }
            set {
            }
        }

        public int GetCoreCount()
        {
            throw new NotImplementedException();
        }

        public float GetFreeMemoryMB()
        {
            throw new NotImplementedException();
        }

        public int GetProcessID()
        {
            throw new NotImplementedException();
        }

        public void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory)
        {
            throw new NotImplementedException();
        }

        public void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string jobInfo, bool forceLogToBrokerDB)
        {
            throw new NotImplementedException();
        }

        public void UpdateAndWrite(EnumMgrStatus mgrStatus, EnumTaskStatus taskStatus, EnumTaskStatusDetail taskDetailStatus, float PercentComplete, int DTACount, string MostRecentLogMessage, string MostRecentErrorMessage, string MostRecentJobInfo, bool ForceLogToBrokerDB) {
            // This would update a status file; instead, do nothing
            return;
        }

        public void UpdateAndWrite(EnumTaskStatus Status, float PercentComplete, int SpectrumCount) {
            // This would update a status file; instead, do nothing
            return;
        }

        public void UpdateAndWrite(EnumMgrStatus mgrStatus, EnumTaskStatus taskStatus, EnumTaskStatusDetail taskDetailStatus, float PercentComplete) {
            // This would update a status file; instead, do nothing
            return;
        }

        public void UpdateAndWrite(float PercentComplete) {
            // This would update a status file; instead, do nothing
            return;
        }

        public void UpdateClose(string ManagerIdleMessage, ref string[] RecentErrorMessages, string MostRecentJobInfo, bool ForceLogToBrokerDB) {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(EnumMgrStatus ManagerStatus, string ManagerDisableMessage, ref string[] RecentErrorMessages, string MostRecentJobInfo) {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(EnumMgrStatus ManagerStatus, string ManagerDisableMessage) {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo)
        {
            throw new NotImplementedException();
        }

        public void UpdateIdle(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo, bool forceLogToBrokerDB)
        {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(EnumMgrStatus ManagerStatus) {
            throw new NotImplementedException();
        }

        public void UpdateFlagFileExists(ref string[] RecentErrorMessages, string MostRecentJobInfo) {
            throw new NotImplementedException();
        }

        public void UpdateFlagFileExists() {
            throw new NotImplementedException();
        }

        public void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string recentJobInfo)
        {
            throw new NotImplementedException();
        }

        public void UpdateIdle(string ManagerIdleMessage, ref string[] RecentErrorMessages, string MostRecentJobInfo, bool ForceLogToBrokerDB) {
            throw new NotImplementedException();
        }

        public void UpdateIdle(string ManagerIdleMessage, string IdleErrorMessage, string MostRecentJobInfo, bool ForceLogToBrokerDB) {
            throw new NotImplementedException();
        }

        public void UpdateIdle(string ManagerIdleMessage, bool ForceLogToBrokerDB) {
            throw new NotImplementedException();
        }

        public void UpdateIdle() {
            throw new NotImplementedException();
        }

        public void WriteStatusFile(bool ForceLogToBrokerDB) {
            throw new NotImplementedException();
        }

        public bool LogToBrokerQueue { get; }
        public string BrokerDBConnectionString { get; }

        public void WriteStatusFile() {
            throw new NotImplementedException();
        }

        #endregion
    }

}
