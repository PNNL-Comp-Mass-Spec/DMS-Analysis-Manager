Imports AnalysisManagerBase.clsGlobal
Imports PRISM.Logging

Public Class clsICR2LSWrapper

	Public Enum BRUKERCHK As Short
		BRUKER_CHK_SER = 3
		BRUKER_CHK_NOSER = 2
		BRUKER_CHK_NONE = 1
	End Enum

	Dim m_Icr2lsObj As icr2ls.ICR2LScls
	Dim m_BrukerFlag As icr2ls.BrukerChkType
	Dim m_DebugLevel As Short = 0
	Dim m_Logger As ILogger

	Sub New(ByVal Logger As ILogger)
		m_Icr2lsObj = New icr2ls.ICR2LScls
		m_BrukerFlag = icr2ls.BrukerChkType.bchkNone		'Default value
		m_Logger = Logger
	End Sub

	Public Sub CloseICR2LS()
		m_Icr2lsObj = Nothing
	End Sub

	Public ReadOnly Property Status() As icr2ls.States
		Get
			Return m_Icr2lsObj.ICR2LSstatus
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
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath)", ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Dataset: " & Dataset, ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Outpath: " & OutPath, ILogger.logMsgType.logDebug, True)
			End If
			m_Icr2lsObj.BrukerChk = m_BrukerFlag
			If UseParmFile Then m_Icr2lsObj.MassTransformParameter = ParmFileNamePath
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MassTransformParameter set", ILogger.logMsgType.logDebug, True)
			End If
			ResCode = m_Icr2lsObj.GenerateTICFileEx(Dataset, OutPath, False, "")
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath)", ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Rescode: " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
			End If
			If ResCode <> 0 Then
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath): " & Err.Message, ILogger.logMsgType.logError, True)
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
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath,ParmFileNamePath)", ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Parmfile: " & ParmFileNamePath, ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Dataset: " & Dataset, ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Outpath: " & OutPath, ILogger.logMsgType.logDebug, True)
			End If
			m_Icr2lsObj.MassTransformParameter = ParmFileNamePath
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MassTransformParameter set", ILogger.logMsgType.logDebug, True)
			End If
			ResCode = m_Icr2lsObj.GenerateTICFileEx(Dataset, OutPath, True, "a")
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath,ParmFileNamePath)", ILogger.logMsgType.logDebug, True)
				m_Logger.PostEntry("Rescode: " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
			End If
			If ResCode <> 0 Then
				If m_DebugLevel > 0 Then
					m_Logger.PostEntry("MakeICRTICFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeICRTICFile(Dataset,OutPath,ParmFileNamePath): " & Err.Message, _
					ILogger.logMsgType.logError, True)
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
				If m_DebugLevel > 0 Then m_Logger.PostEntry("MakeLCQTICFile, ResCode = " & ResCode.ToString, _
					ILogger.logMsgType.logDebug, True)
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeLCQTICFile, " & Err.Message, ILogger.logMsgType.logError, True)
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
				If m_DebugLevel > 0 Then
					m_Logger.PostEntry("MakeICRPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return True
				End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeICRPEKfile, " & Err.Message, ILogger.logMsgType.logError, True)
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
					m_Logger.PostEntry("MakeQTOFPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeQTOFPEKFile, " & Err.Message, ILogger.logMsgType.logError, True)
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
					m_Logger.PostEntry("MakeMMTOFPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeMMTOFPEKFile, " & Err.Message, ILogger.logMsgType.logError, True)
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
					m_Logger.PostEntry("MakeLTQ_FTPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeLTQ_FTPEKFile, " & Err.Message, ILogger.logMsgType.logError, True)
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
					m_Logger.PostEntry("MakeMMTOFPEKFile, ResCode = " & ResCode.ToString, ILogger.logMsgType.logDebug, True)
				End If
				Return False
			Else
				Return True
			End If
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("MakeMMTOFPEKFile, " & Err.Message, ILogger.logMsgType.logError, True)
			End If
			Return False
		End Try

	End Function

End Class
