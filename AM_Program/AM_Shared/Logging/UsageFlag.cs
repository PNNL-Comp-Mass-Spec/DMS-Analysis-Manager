using System;
using System.Collections.Generic;

namespace AnalysisManagerBase.Logging
{
    class UsageFlag : IDisposable
    {
        private static readonly Dictionary<string, int> mFlagUsers = new Dictionary<string, int>();

        readonly string mFlagName;

        public static bool InUse(string name)
        {
            lock (mFlagUsers)
                return (mFlagUsers.ContainsKey(name) && mFlagUsers[name] > 0);
        }

        public UsageFlag(string name)
        {
            mFlagName = name;

            lock (mFlagUsers)
            {
                if (!mFlagUsers.ContainsKey(name))
                    mFlagUsers.Add(name, 0);

                mFlagUsers[name] += 1;
            }
        }

        public void Dispose()
        {
            lock (mFlagUsers)
                mFlagUsers[mFlagName] -= 1;
        }
    }
}
