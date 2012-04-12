using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;

namespace TestApePlugIn {

    class StatusFileStub : IStatusFile {
        #region IStatusFile Members

        public int CpuUtilization {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string CurrentOperation {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string Dataset {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string FileNamePath {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public int JobNumber {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public int JobStep {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public bool LogToMsgQueue {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string MessageQueueTopic {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string MessageQueueURI {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string MgrName {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public IStatusFile.EnumMgrStatus MgrStatus {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string MostRecentJobInfo {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public float Progress {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public int SpectrumCount {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public IStatusFile.EnumTaskStatus TaskStatus {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public IStatusFile.EnumTaskStatusDetail TaskStatusDetail {
            get {
                throw new NotImplementedException();
            }
            set {
                throw new NotImplementedException();
            }
        }

        public string Tool {
            get {
                return "Stub";
            }
            set {
            }
        }

        public void UpdateAndWrite(IStatusFile.EnumMgrStatus mgrStatus, IStatusFile.EnumTaskStatus taskStatus, IStatusFile.EnumTaskStatusDetail taskDetailStatus, float PercentComplete, int DTACount, string MostRecentLogMessage, string MostRecentErrorMessage, string MostRecentJobInfo, bool ForceLogToBrokerDB) {
			// This would update a status file; instead, do nothing
			return;
        }

        public void UpdateAndWrite(IStatusFile.EnumTaskStatus Status, float PercentComplete, int SpectrumCount) {
			// This would update a status file; instead, do nothing
			return;
        }

        public void UpdateAndWrite(IStatusFile.EnumMgrStatus mgrStatus, IStatusFile.EnumTaskStatus taskStatus, IStatusFile.EnumTaskStatusDetail taskDetailStatus, float PercentComplete) {
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

        public void UpdateDisabled(IStatusFile.EnumMgrStatus ManagerStatus, string ManagerDisableMessage, ref string[] RecentErrorMessages, string MostRecentJobInfo) {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(IStatusFile.EnumMgrStatus ManagerStatus, string ManagerDisableMessage) {
            throw new NotImplementedException();
        }

        public void UpdateDisabled(IStatusFile.EnumMgrStatus ManagerStatus) {
            throw new NotImplementedException();
        }

        public void UpdateFlagFileExists(ref string[] RecentErrorMessages, string MostRecentJobInfo) {
            throw new NotImplementedException();
        }

        public void UpdateFlagFileExists() {
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

        public void WriteStatusFile() {
            throw new NotImplementedException();
        }

        #endregion
    }

}
