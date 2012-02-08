'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/29/2009
'
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsCreateInspectIndexedDB

    ''' <summary>
    ''' Convert .Fasta file to indexed DB files
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function CreateIndexedDbFiles(ByRef mgrParams As AnalysisManagerBase.IMgrParams, _
                                         ByRef jobParams As AnalysisManagerBase.IJobParams, _
                                         ByVal DebugLevel As Integer, _
                                         ByVal JobNum As String, _
                                         ByVal InspectDir As String, _
                                         ByVal OrgDbDir As String) As IJobParams.CloseOutType

        Const MAX_WAITTIME_HOURS As Single = 1.0
        Const MAX_WAITTIME_PREVENT_REPEATS As Single = 2.0

        Const PREPDB_SCRIPT As String = "PrepDB.py"
        Const SHUFFLEDB_SCRIPT As String = "ShuffleDB_Seed.py"

        Dim objPrepDB As clsRunDosProgram
        Dim objShuffleDB As clsRunDosProgram
        Dim CmdStr As String

        Dim intRandomNumberSeed As Integer
        Dim blnShuffleDBPreventRepeats As Boolean

        Dim strDBFileNameInput As String
        Dim strOutputNameBase As String

        Dim dbLockFilename As String
        Dim dbTrieFilenameBeforeShuffle As String
        Dim dbTrieFilename As String

        Dim pythonProgLoc As String
        Dim blnUseShuffledDB As Boolean

        Dim fi As System.IO.FileInfo
        Dim createTime As DateTime
        Dim durationTime As TimeSpan
        Dim currentTime As DateTime
        Dim sngMaxWaitTimeHours As Single = MAX_WAITTIME_HOURS

        Try
            If DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateInspectIndexedDB.CreateIndexedDbFiles(): Enter")
            End If

            intRandomNumberSeed = AnalysisManagerBase.clsGlobal.CIntSafe(jobParams.GetParam("InspectShuffleDBSeed"), 1000)
            blnShuffleDBPreventRepeats = AnalysisManagerBase.clsGlobal.CBoolSafe(jobParams.GetParam("InspectPreventShuffleDBRepeats"), False)

            If blnShuffleDBPreventRepeats Then
                sngMaxWaitTimeHours = MAX_WAITTIME_PREVENT_REPEATS
            End If

            strDBFileNameInput = System.IO.Path.Combine(OrgDbDir, jobParams.GetParam("PeptideSearch", "generatedFastaName"))
            blnUseShuffledDB = AnalysisManagerBase.clsGlobal.CBoolSafe(jobParams.GetParam("InspectUsesShuffledDB"), False)

            strOutputNameBase = System.IO.Path.GetFileNameWithoutExtension(strDBFileNameInput)
            dbTrieFilenameBeforeShuffle = System.IO.Path.Combine(OrgDbDir, strOutputNameBase & ".trie")

            If blnUseShuffledDB Then
                ' Will create the .trie file using PrepDB.py, then shuffle it using shuffleDB.py
                ' The Pvalue.py script does much better at computing p-values if a decoy search is performed (i.e. shuffleDB.py is used)
                ' Note that shuffleDB will add a prefix of XXX to the shuffled protein names
                strOutputNameBase &= "_shuffle"
            End If

            dbLockFilename = System.IO.Path.Combine(OrgDbDir, strOutputNameBase & "_trie.lock")
            dbTrieFilename = System.IO.Path.Combine(OrgDbDir, strOutputNameBase & ".trie")

            pythonProgLoc = mgrParams.GetParam("pythonprogloc")

            objPrepDB = New clsRunDosProgram(InspectDir & System.IO.Path.DirectorySeparatorChar)

            ' Check to see if another Analysis Manager is already creating the indexed db files
            If System.IO.File.Exists(dbLockFilename) Then
                If DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Lock file found: " & dbLockFilename & "; waiting for file to be removed by other manager generating .trie file " & System.IO.Path.GetFileName(dbTrieFilename))
                End If

                ' Lock file found; wait up to sngMaxWaitTimeHours hours
                fi = My.Computer.FileSystem.GetFileInfo(dbLockFilename)
                createTime = fi.CreationTimeUtc
                currentTime = System.DateTime.UtcNow
                durationTime = currentTime - createTime
                While System.IO.File.Exists(dbLockFilename) And durationTime.Hours < sngMaxWaitTimeHours
                    ' Sleep for 2 seconds
                    System.Threading.Thread.Sleep(2000)

                    ' Update the current time and elapsed duration
                    currentTime = System.DateTime.UtcNow
                    durationTime = currentTime - createTime
                End While

                'If the duration time has exceeded sngMaxWaitTimeHours, then delete the lock file and try again with this manager
                If durationTime.Hours > sngMaxWaitTimeHours Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Waited over " & sngMaxWaitTimeHours.ToString("0.0") & " hour(s) for lock file: " & dbLockFilename & " to be deleted, but it is still present; deleting the file now and continuing")
                    If System.IO.File.Exists(dbLockFilename) Then
                        System.IO.File.Delete(dbLockFilename)
                    End If
                End If

            End If

            ' If lock file existed, the index files should now be created
            ' Check for one of the index files in case this is the first time or there was a problem with 
            ' another manager creating it.
            If Not System.IO.File.Exists(dbTrieFilename) Then
                ' Try to create the index files for fasta file strDBFileNameInput

                ' Verify that python program file exists
                If Not System.IO.File.Exists(pythonProgLoc) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & pythonProgLoc)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                ' Verify that the PrepDB python script exists
                Dim PrebDBScriptPath As String = System.IO.Path.Combine(InspectDir, PREPDB_SCRIPT)
                If Not System.IO.File.Exists(PrebDBScriptPath) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PrepDB script: " & PrebDBScriptPath)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                ' Verify that the ShuffleDB python script exists
                Dim ShuffleDBScriptPath As String = System.IO.Path.Combine(InspectDir, SHUFFLEDB_SCRIPT)
                If blnUseShuffledDB Then
                    If Not System.IO.File.Exists(ShuffleDBScriptPath) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find ShuffleDB script: " & ShuffleDBScriptPath)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                End If

                If DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating lock file: " & dbLockFilename)
                End If

                'Create lock file
                Dim bSuccess As Boolean
                bSuccess = CreateLockFile(dbLockFilename)
                If Not bSuccess Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                If DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating indexed database file: " & dbTrieFilenameBeforeShuffle)
                End If

                'Set up and execute a program runner to run PrepDB.py
                CmdStr = " " & PrebDBScriptPath & " FASTA " & strDBFileNameInput
                If DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, pythonProgLoc & " " & CmdStr)
                End If

                If Not objPrepDB.RunProgram(pythonProgLoc, CmdStr, "PrepDB", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running " & PREPDB_SCRIPT & " for " & strDBFileNameInput & " : " & JobNum)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                Else
                    If DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Created .trie file for " & strDBFileNameInput)
                    End If
                End If

                If blnUseShuffledDB Then
                    'Set up and execute a program runner to run ShuffleDB_seed.py
                    objShuffleDB = New clsRunDosProgram(InspectDir & System.IO.Path.DirectorySeparatorChar)

                    CmdStr = " " & ShuffleDBScriptPath & " -r " & dbTrieFilenameBeforeShuffle & " -w " & dbTrieFilename

                    If blnShuffleDBPreventRepeats Then
                        CmdStr &= " -p"
                    End If

                    If intRandomNumberSeed <> 0 Then
                        CmdStr &= " -d " & intRandomNumberSeed.ToString
                    End If

                    If DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, pythonProgLoc & " " & CmdStr)
                    End If

                    If Not objShuffleDB.RunProgram(pythonProgLoc, CmdStr, "ShuffleDB", True) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running " & SHUFFLEDB_SCRIPT & " for " & dbTrieFilenameBeforeShuffle & " : " & JobNum)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    Else
                        If DebugLevel >= 1 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Shuffled .trie file created: " & dbTrieFilename)
                        End If
                    End If
                End If


                If DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting lock file: " & dbLockFilename)
                End If

                ' Delete the lock file
                If System.IO.File.Exists(dbLockFilename) Then
                    System.IO.File.Delete(dbLockFilename)
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateInspectIndexedDB.CreateIndexedDbFiles, An exception has occurred: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Creates a lock file
    ''' </summary>
    ''' <returns>True if success; false if failure</returns>
    Protected Function CreateLockFile(ByVal strLockFilePath As String) As Boolean

        Try
            Dim sw As System.IO.StreamWriter = New System.IO.StreamWriter(strLockFilePath)

            ' Add Date and time to the file.
            sw.WriteLine(DateTime.Now)
            sw.Close()
            sw.Dispose()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateInspectIndexedDB.CreateLockFile, Error creating lock file: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

End Class
