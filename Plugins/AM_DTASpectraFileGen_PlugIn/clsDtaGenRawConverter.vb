'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/21/2016
'
' Uses RawConverter to create a .MGF file from a .Raw file (He and Yates. Anal Chem. 2015 Nov 17; 87 (22): 11361-11367)
' Next, converts the .MGF file to a _DTA.txt file
'*********************************************************************************************************


Imports AnalysisManagerBase
Imports System.IO

Public Class clsDtaGenRawConverter
    Inherits clsDtaGenThermoRaw

    Public Overrides Sub Setup(initParams As ISpectraFileProcessor.InitializationParams, toolRunner As clsAnalysisToolRunnerBase)
        MyBase.Setup(initParams, toolRunner)
    End Sub

    ''' <summary>
    ''' Returns the default path to the DTA generator tool
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
    Protected Overrides Function ConstructDTAToolPath() As String

        Dim strDTAToolPath As String

        Dim rawConverterDir As String = m_MgrParams.GetParam("RawConverterProgLoc")
        strDTAToolPath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME)

        Return strDTAToolPath

    End Function

    Protected Overrides Sub MakeDTAFilesThreaded()

        m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
        m_ErrMsg = String.Empty

        m_Progress = 10

        If Not ConvertRawToMGF(m_RawDataType) Then
            If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
                m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
                m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            End If
            Return
        End If

        m_Progress = 75

        If Not ConvertMGFtoDTA() Then
            If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
                m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
                m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            End If
            Return
        End If

        m_Results = ISpectraFileProcessor.ProcessResults.SF_SUCCESS
        m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE

    End Sub

    ''' <summary>
    ''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
    ''' This function is called by MakeDTAFilesThreaded
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function ConvertMGFtoDTA() As Boolean

        Try
            Dim strRawDataType As String = m_JobParams.GetJobParameter("RawDataType", "")

            Dim oMGFConverter = New clsMGFConverter(m_DebugLevel, m_WorkDir) With {
                .IncludeExtraInfoOnParentIonLine = True,
                .MinimumIonsPerSpectrum = m_JobParams.GetJobParameter("IonCounts", "IonCount", 0)
            }

            Dim eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)
            Dim blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, m_Dataset)

            If Not blnSuccess Then
                m_ErrMsg = oMGFConverter.ErrorMessage
            End If

            m_SpectraFileCount = oMGFConverter.SpectraCountWritten
            m_Progress = 95

            Return blnSuccess

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ConvertMGFtoDTA: " + ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Create .mgf file using RawConverter
    ''' This function is called by MakeDTAFilesThreaded
    ''' </summary>
    ''' <param name="eRawDataType">Raw data file type</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function ConvertRawToMGF(eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As Boolean

        Try

            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .MGF file using RawConverter")
            End If

            Dim rawFilePath As String

            ' Construct the path to the .raw file
            Select Case eRawDataType
                Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                    rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
                Case Else
                    m_ErrMsg = "Raw data file type not supported: " & eRawDataType.ToString()
                    Return False
            End Select

            m_InstrumentFileName = Path.GetFileName(rawFilePath)
            m_JobParams.AddResultFileToSkip(m_InstrumentFileName)

            Dim fiRawConverter = New FileInfo(m_DtaToolNameLoc)

            ' Set up command
            Dim cmdStr = " " & rawFilePath & " --mgf"

            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_DtaToolNameLoc & " " & cmdStr)
            End If

            ' Setup a program runner tool to make the spectra files
            ' The working directory must be the folder that has RawConverter.exe
            ' Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

            m_RunProgTool = New clsRunDosProgram(fiRawConverter.Directory.FullName) With {
                .CreateNoWindow = True,
                .CacheStandardOutput = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = True,
                .ConsoleOutputFilePath = String.Empty      ' Allow the console output filename to be auto-generated
            }

            If Not m_RunProgTool.RunProgram(m_DtaToolNameLoc, cmdStr, "RawConverter", True) Then
                ' .RunProgram returned False
                LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False")

                m_ErrMsg = "Error running " & Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
                Return False
            End If

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... MGF file created using RawConverter")
            End If

            Return True

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ConvertRawToMGF: " + ex.Message)
            Return False
        End Try

    End Function


End Class
