' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase.clsGlobal

Public Class clsICR2LSWrapper

	Public Enum BRUKERCHK As Short
		BRUKER_CHK_SER = 3
		BRUKER_CHK_NOSER = 2
		BRUKER_CHK_NONE = 1
	End Enum

    Public Enum ICR2LSState
        Idle = 1           ' 1
        Processing      ' 2
        Killed          ' 3
        Finished        ' 4
        PekGeneration ' 5
        TICGeneration ' 6
        LCQTICGeneration ' 7
        QTOFPekGeneration ' 8
        MMPekGeneration ' 9
        LTQFTPekGeneration ' 10
        Unknown
    End Enum

    Dim m_Icr2lsObj As icr2ls.ICR2LScls
    Dim m_BrukerFlag As icr2ls.BrukerChkType
    Dim m_DebugLevel As Short = 0

    Sub New()
        m_Icr2lsObj = New icr2ls.ICR2LScls
        m_BrukerFlag = icr2ls.BrukerChkType.bchkNone        'Default value
    End Sub

    Public Sub CloseICR2LS()
        m_Icr2lsObj = Nothing
    End Sub

    Public ReadOnly Property Status() As clsAnalysisToolRunnerICRBase.ICR_STATUS
        Get
            Select Case m_Icr2lsObj.ICR2LSstatus
                Case icr2ls.States.sIdle            ' 1
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_IDLE

                Case icr2ls.States.sProcessing      ' 2
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_PROCESSING

                Case icr2ls.States.sKilled          ' 3
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_KILLED

                Case icr2ls.States.sFinished        ' 4
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_FINISHED

                Case icr2ls.States.sPekGeneration ' 5
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_GENERATING

                Case icr2ls.States.sTICGeneration ' 6
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_TICGENERATION

                Case icr2ls.States.sLCQTICGeneration ' 7
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_LCQTICGENERATION

                Case icr2ls.States.sQTOFPekGeneration ' 8
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_QTOFPEKGENERATION

                Case icr2ls.States.sMMPekGeneration ' 9
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_MMTOFPEKGENERATION

                Case icr2ls.States.sLTQFTPekGeneration ' 10
                    Return clsAnalysisToolRunnerICRBase.ICR_STATUS.STATE_LTQFTPEKGENERATION

                Case Else
                    ' Unknown state
                    Return 0
            End Select
        End Get
    End Property

    Public ReadOnly Property Progress() As Single
        Get
            Return m_Icr2lsObj.MassTransformProgress
        End Get
    End Property

    'Flag for Bruker settings (not needed if bruker data has been converted to s-folder format)
    Public WriteOnly Property BrukerFlag() As BRUKERCHK
        Set(ByVal Value As BRUKERCHK)
            Select Case Value
                Case BRUKERCHK.BRUKER_CHK_NONE
                    m_BrukerFlag = icr2ls.BrukerChkType.bchkNone
                Case BRUKERCHK.BRUKER_CHK_NOSER
                    m_BrukerFlag = icr2ls.BrukerChkType.bchkBruker
                Case BRUKERCHK.BRUKER_CHK_SER
                    m_BrukerFlag = icr2ls.BrukerChkType.bchkBrukerSER
                Case Else
                    'Shouldn't ever get to here
            End Select
        End Set
    End Property

    Public Overloads Function MakeICRTICFile(ByVal Dataset As String, ByVal OutPath As String, _
     ByVal ParmFileNamePath As String, ByVal UseParmFile As Boolean) As Boolean

        'Makes a TIC file for a FTICR dataset without Bruker base settings turned on
        Dim ResCode As Short

        Try
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeICRTICFile(Dataset,OutPath)")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset: " & Dataset)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Outpath: " & OutPath)
            End If
            m_Icr2lsObj.BrukerChk = m_BrukerFlag
            If UseParmFile Then m_Icr2lsObj.MassTransformParameter = ParmFileNamePath
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MassTransformParameter set")
            End If
            ResCode = m_Icr2lsObj.GenerateTICFileEx(Dataset, OutPath, False, "")
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeICRTICFile(Dataset,OutPath)")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Rescode: " & ResCode.ToString)
            End If
            If ResCode <> 0 Then
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeICRTICFile(Dataset,OutPath): " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Overloads Function MakeICRTICFile(ByVal Dataset As String, ByVal OutPath As String, _
     ByVal ParmFileNamePath As String) As Boolean

        'Makes a TIC file for a FTICR dataset with Bruker base settings turned on
        Dim ResCode As Short

        Try
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeICRTICFile(Dataset,OutPath,ParmFileNamePath)")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parmfile: " & ParmFileNamePath)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset: " & Dataset)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Outpath: " & OutPath)
            End If
            m_Icr2lsObj.MassTransformParameter = ParmFileNamePath
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MassTransformParameter set")
            End If
            ResCode = m_Icr2lsObj.GenerateTICFileEx(Dataset, OutPath, True, "a")
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeICRTICFile(Dataset,OutPath,ParmFileNamePath)")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Rescode: " & ResCode.ToString)
            End If
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeICRTICFile, ResCode = " & ResCode.ToString)
                End If
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeICRTICFile(Dataset,OutPath,ParmFileNamePath): " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Function MakeLCQTICFIle(ByVal Dataset As String, ByVal OutPath As String) As Boolean

        'Makes a TIC file from a Finnigan LCQ .raw file
        Dim ResCode As Short

        Try
            m_Icr2lsObj.BrukerChk = m_BrukerFlag
            ResCode = m_Icr2lsObj.GenerateLCQTICFile(Dataset, OutPath)
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeLCQTICFile, ResCode = " & ResCode.ToString)
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeLCQTICFile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Function MakeICRPEKFile(ByVal Dataset As String, ByVal ParmFile As String, _
    ByVal Outpath As String) As Boolean

        'Makes a PEK file from an ICR file in S-folder format
        Dim ResCode As Short

        Try
            ResCode = m_Icr2lsObj.GeneratePekFile(Dataset, ParmFile, Outpath)
            If ResCode <> 0 Then
                Return False
            Else
                'If m_DebugLevel > 0 Then
                '	m_Logger.PostEntry("MakeICRPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
                'End If
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeICRPEKfile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Function MakeQTOFPEKFile(ByVal DataSet As String, ByVal ParmFile As String, _
     ByVal OutPath As String, ByVal SGFilter As Boolean) _
     As Boolean

        'Note: the SGFilter parameter was deleted by ICR2LS V2.30.0.87, but has been left in this function
        '	parameter list in case it comes back in the future

        'Makes a PEK file from a QTOF input file
        Dim ResCode As Short

        Try
            ResCode = m_Icr2lsObj.GenerateQstarPekFile(DataSet, ParmFile, OutPath)
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeQTOFPEKFile, ResCode = " & ResCode.ToString)
                End If
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeQTOFPEKFile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Function MakeMMTOFPEKFile(ByVal DataSet As String, ByVal ParmFile As String, _
     ByVal OutFile As String, ByVal FilterNum As Integer, ByVal SGFilter As Boolean) _
     As Boolean

        'Makes a PEK file from a QTOF input file
        Dim ResCode As Short

        Try
            ResCode = m_Icr2lsObj.GenerateMicroMassPekFile(DataSet, ParmFile, OutFile, FilterNum, SGFilter)
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeMMTOFPEKFile, ResCode = " & ResCode.ToString)
                End If
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeMMTOFPEKFile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public Overloads Function MakeLTQ_FTPEKFile(ByVal DataFileName As String, ByVal ParmFile As String, _
     ByVal OutFileName As String) As Boolean

        'Makes a PEK file from a LTQ-FT input file
        '
        'Overloaded version that can be used when processing of all scans is desired

        Return Me.MakeLTQ_FTPEKFile(DataFileName, ParmFile, OutFileName, True, 1, 1)

    End Function

    Public Overloads Function MakeLTQ_FTPEKFile(ByVal DataFileName As String, ByVal ParmFile As String, _
     ByVal OutFileName As String, ByVal ProcessAll As Boolean, ByVal NumScans As Integer, _
     ByVal StartScan As Integer) As Boolean

        'Makes a PEK file from a LTQ-FT input file
        '
        'This is the full function that's called by the overloaded version above

        Dim ResCode As Short

        Try
            ResCode = m_Icr2lsObj.GenerateLTQFTPekFile(DataFileName, ParmFile, OutFileName, ProcessAll, NumScans, StartScan)
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeLTQ_FTPEKFile, ResCode = " & ResCode.ToString)
                End If
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeLTQ_FTPEKFile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

    Public WriteOnly Property DebugLevel() As Short
        Set(ByVal Value As Short)
            m_DebugLevel = Value
        End Set
    End Property

    Public Function MakeAgilentTOFPEKFile(ByVal DataSet As String, ByVal ParmFile As String, _
     ByVal OutFile As String, ByVal SGFilter As Boolean) _
     As Boolean

        'Note: the SGFilter parameter was deleted by ICR2LS V2.30.0.87, but has been left in this function
        '	parameter list in case it comes back in the future

        'Makes a PEK file from a QTOF input file
        Dim ResCode As Short

        Try
            ResCode = m_Icr2lsObj.GenerateAgilentPekFile(DataSet, ParmFile, OutFile)
            If ResCode <> 0 Then
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MakeMMTOFPEKFile, ResCode = " & ResCode.ToString)
                End If
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MakeMMTOFPEKFile, " & Err.Message)
            End If
            Return False
        End Try

    End Function

End Class
