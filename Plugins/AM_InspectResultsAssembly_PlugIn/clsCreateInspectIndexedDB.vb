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
    Public Function CreateIndexedDbFiles(ByRef mgrParams As IMgrParams, _
                                         ByRef jobParams As IJobParams, _
                                         ByVal DebugLevel As Integer, _
                                         ByVal JobNum As String) As IJobParams.CloseOutType

        Const PREPDB_SCRIPT As String = "PrepDB.py"

        Dim CmdRunner As clsRunDosProgram
        Dim CmdStr As String

        Dim WorkingDir As String = mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = mgrParams.GetParam("inspectdir")
        Dim orgDbDir As String = mgrParams.GetParam("orgdbdir")
        Dim dbFilename As String = System.IO.Path.Combine(orgDbDir, jobParams.GetParam("PeptideSearch", "generatedFastaName"))
        Dim dbLockFilename As String = System.IO.Path.Combine(orgDbDir, System.IO.Path.GetFileNameWithoutExtension(dbFilename) & "_trie.lock")
        Dim dbTrieFilename As String = System.IO.Path.Combine(orgDbDir, System.IO.Path.GetFileNameWithoutExtension(dbFilename) & ".trie")
        Dim pythonProgLoc As String = mgrParams.GetParam("pythonprogloc")
        Dim fi As System.IO.FileInfo
        Dim createTime As DateTime
        Dim result As IJobParams.CloseOutType
        Dim durationTime As TimeSpan
        Dim currentTime As DateTime

        Try
            CmdRunner = New clsRunDosProgram(InspectDir & System.IO.Path.DirectorySeparatorChar)

            If DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateInspectIndexedDB.CreateIndexedDbFiles(): Enter")
            End If

            ' Check to see if another Analysis Manager is already creating the indexed db files
            If System.IO.File.Exists(dbLockFilename) Then
                If DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Lock file found: " & dbLockFilename & "; waiting for file to be removed by other manager generating .trie file " & System.IO.Path.GetFileName(dbTrieFilename))
                End If

                ' Lock file found; wait up to one hour
                fi = My.Computer.FileSystem.GetFileInfo(dbLockFilename)
                createTime = fi.CreationTimeUtc
                currentTime = System.DateTime.UtcNow
                durationTime = currentTime - createTime
                While System.IO.File.Exists(dbLockFilename) And durationTime.Hours < 1
                    ' Sleep for 2 seconds
                    System.Threading.Thread.Sleep(2000)

                    ' Update the current time and elapsed duration
                    currentTime = System.DateTime.UtcNow
                    durationTime = currentTime - createTime
                End While

                'If the duration time has exceeded 1 hour, then delete the lock file and try again with this manager
                If durationTime.Hours > 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Waited over 1 hour for lock file: " & dbLockFilename & " to be deleted, but it is still present; deleting the file now and continuing" & System.IO.Path.GetFileName(dbTrieFilename))

                    If System.IO.File.Exists(dbLockFilename) Then
                        System.IO.File.Delete(dbLockFilename)
                    End If
                End If

            End If

            'if lock file existed, the index files should now be created
            'Check for one of the index files in case this is the first time or there was a problem with 
            'another manager creating it.
            If Not System.IO.File.Exists(dbTrieFilename) Then
                If DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating lock file: " & dbLockFilename)
                End If

                'Try to create the index files again
                'Create lock file
                result = CreateLockFile(dbLockFilename)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If

                ' verify that python program file exists
                Dim progLoc As String = pythonProgLoc
                If Not System.IO.File.Exists(progLoc) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & progLoc)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                ' verify that PrepDB python script exists
                Dim PrebDBScriptPath As String = System.IO.Path.Combine(InspectDir, PREPDB_SCRIPT)
                If Not System.IO.File.Exists(PrebDBScriptPath) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PrepDB script: " & PrebDBScriptPath)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                If DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating indexed database file: " & dbTrieFilename)
                End If

                'Set up and execute a program runner to run PrepDB.py
                CmdStr = " " & PrebDBScriptPath & " FASTA " & dbFilename
                If DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
                End If

                If Not CmdRunner.RunProgram(progLoc, CmdStr, "PrepDB", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running " & PREPDB_SCRIPT & " for " & dbFilename & " : " & JobNum)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateInspectIndexedDB.CreateIndexedDbFiles An exception has occurred: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Creates a lock file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks>Not presently implemented</remarks>
    Protected Function CreateLockFile(ByVal lockFilename As String) As IJobParams.CloseOutType

        Try
            Dim sw As System.IO.StreamWriter = New System.IO.StreamWriter(lockFilename)
            ' Add Date and time to the file.
            sw.WriteLine(DateTime.Now)
            sw.Close()
            sw.Dispose()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateInspectIndexedDB.CreateLockFile, Error creating lock file: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
