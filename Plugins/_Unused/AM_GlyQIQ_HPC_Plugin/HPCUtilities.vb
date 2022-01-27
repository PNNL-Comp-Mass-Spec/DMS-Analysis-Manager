Imports System.IO
Imports AnalysisManagerBase.JobConfig

Public Class HPCUtilities

    Protected Const FORCE_WINHPC_FS As Boolean = True

    Public Structure udtHPCOptionsType
        Public HeadNode As String
        Public UsingHPC As Boolean
        Public SharePath As String
        Public ResourceType As String
        ' Obsolete parameter; no longer used: Public NodeGroup As String
        Public MinimumMemoryMB As Integer
        Public MinimumCores As Integer
        Public WorkDirPath As String
    End Structure

    Public Shared Function GetHPCOptions(jobParams As IJobParams, managerName As String) As udtHPCOptionsType

        Dim stepTool = jobParams.GetJobParameter("StepTool", "Unknown_Tool")

        Dim udtHPCOptions = New udtHPCOptionsType

        udtHPCOptions.HeadNode = jobParams.GetJobParameter("HPCHeadNode", "")
        If stepTool.ToLower() = "MSGFPlus_HPC".ToLower() AndAlso String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode) Then
            ' Run this job using HPC, despite the fact that the settings file does not have the HPC settings defined
            udtHPCOptions.HeadNode = "deception2.pnnl.gov"
            udtHPCOptions.UsingHPC = True
        Else
            udtHPCOptions.UsingHPC = Not String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode)
        End If

        udtHPCOptions.ResourceType = jobParams.GetJobParameter("HPCResourceType", "socket")
        ' Obsolete parameter; no longer used: udtHPCOptions.NodeGroup = jobParams.GetJobParameter("HPCNodeGroup", "ComputeNodes")

        ' Share paths used:
        ' \\picfs\projects\DMS
        ' \\winhpcfs\projects\DMS           (this is a Windows File System wrapper to \\picfs, which is an Isilon FS)

        If FORCE_WINHPC_FS Then
            udtHPCOptions.SharePath = jobParams.GetJobParameter("HPCSharePath", "\\winhpcfs\projects\DMS")
        Else
            udtHPCOptions.SharePath = jobParams.GetJobParameter("HPCSharePath", "\\picfs.pnl.gov\projects\DMS")
        End If


        ' Auto-switched the share path to \\winhpcfs starting April 15, 2014
        ' Stopped doing this April 21, 2014, because the drive was low on space
        ' Switched back to \\winhpcfs on April 24, because the connection to picfs is unstable
        '

        If FORCE_WINHPC_FS Then
            If udtHPCOptions.SharePath.StartsWith("\\picfs", StringComparison.CurrentCultureIgnoreCase) Then
                ' Auto switch the share path
                udtHPCOptions.SharePath = UpdateHostName(udtHPCOptions.SharePath, "\\winhpcfs\")
            End If
        Else
            If udtHPCOptions.SharePath.StartsWith("\\winhpcfs", StringComparison.CurrentCultureIgnoreCase) Then
                ' Auto switch the share path
                udtHPCOptions.SharePath = UpdateHostName(udtHPCOptions.SharePath, "\\picfs.pnl.gov\")
            End If
        End If

        udtHPCOptions.MinimumMemoryMB = jobParams.GetJobParameter("HPCMinMemoryMB", 0)
        udtHPCOptions.MinimumCores = jobParams.GetJobParameter("HPCMinCores", 0)

        If udtHPCOptions.UsingHPC AndAlso udtHPCOptions.MinimumMemoryMB <= 0 Then
            udtHPCOptions.MinimumMemoryMB = 28000
        End If

        If udtHPCOptions.UsingHPC AndAlso udtHPCOptions.MinimumCores <= 0 Then
            udtHPCOptions.MinimumCores = 16
        End If

        Dim mgrNameClean = String.Empty

        For charIndex = 0 To managerName.Length - 1
            If Path.GetInvalidFileNameChars.Contains(managerName.Chars(charIndex)) Then
                mgrNameClean &= "_"
            Else
                mgrNameClean &= managerName.Chars(charIndex)
            End If
        Next

        ' Example WorkDirPath:
        ' \\picfs.pnl.gov\projects\DMS\DMS_Work_Dir\Pub-60-3
        ' \\winhpcfs\projects\DMS\DMS_Work_Dir\Pub-60-3
        udtHPCOptions.WorkDirPath = Path.Combine(udtHPCOptions.SharePath, "DMS_Work_Dir", mgrNameClean)

        Return udtHPCOptions

    End Function


    ''' <summary>
    ''' Change the host name in the given share path to use a different host
    ''' </summary>
    ''' <param name="sharePath"></param>
    ''' <param name="newHostName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function UpdateHostName(sharePath As String, newHostName As String) As String

        If Not newHostName.StartsWith("\\") Then
            Throw New NotSupportedException("\\ not found at the start of newHostName (" & newHostName & "); The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\")
        End If

        If Not newHostName.EndsWith("\") Then
            newHostName &= "\"
        End If

        If Not sharePath.StartsWith("\\") Then
            Throw New NotSupportedException("\\ not found at the start of sharePath (" & sharePath & "); The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\")
        End If

        Dim slashLoc = sharePath.IndexOf("\", 3, StringComparison.Ordinal)

        If slashLoc < 0 Then
            Throw New Exception("Backslash not found after the 3rd character in SharePath, " & sharePath)
        End If

        Dim sharePathNew = newHostName & sharePath.Substring(slashLoc + 1)

        Return sharePathNew

    End Function
End Class
