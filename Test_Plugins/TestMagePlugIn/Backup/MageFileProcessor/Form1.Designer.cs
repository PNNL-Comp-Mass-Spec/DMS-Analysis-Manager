namespace MageFileProcessor {
    partial class Form1 {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.JoblistSourceTabs = new System.Windows.Forms.TabControl();
            this.QueryTabPage = new System.Windows.Forms.TabPage();
            this.jobListPanel1 = new MageClientLib.JobListPanel();
            this.JobListTabPage = new System.Windows.Forms.TabPage();
            this.jobIDListPanel1 = new MageClientLib.JobIDListPanel();
            this.DataPackageTabPage = new System.Windows.Forms.TabPage();
            this.jobDataPackagePanel1 = new MageClientLib.JobDataPackagePanel();
            this.panel1 = new System.Windows.Forms.Panel();
            this.JobListDisplayControl = new MageClientLib.ListDisplayControl();
            this.panel2 = new System.Windows.Forms.Panel();
            this.FileListDisplayControl = new MageClientLib.ListDisplayControl();
            this.FileSourceTabs = new System.Windows.Forms.TabControl();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.jobFilePanel1 = new MageClientLib.JobFilePanel();
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.localFolderPanel1 = new MageClientLib.LocalFolderPanel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.panel4 = new System.Windows.Forms.Panel();
            this.FilterOutputTabs = new System.Windows.Forms.TabControl();
            this.tabPage3 = new System.Windows.Forms.TabPage();
            this.folderDestinationPanel1 = new MageClientLib.FolderDestinationPanel();
            this.tabPage4 = new System.Windows.Forms.TabPage();
            this.sqLiteDestinationPanel1 = new MageClientLib.SQLiteDestinationPanel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.fileProcessingPanel1 = new MageClientLib.FileProcessingPanel();
            this.splitter1 = new System.Windows.Forms.Splitter();
            this.splitContainer1 = new System.Windows.Forms.SplitContainer();
            this.JoblistSourceTabs.SuspendLayout();
            this.QueryTabPage.SuspendLayout();
            this.JobListTabPage.SuspendLayout();
            this.DataPackageTabPage.SuspendLayout();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.FileSourceTabs.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.panel3.SuspendLayout();
            this.panel4.SuspendLayout();
            this.FilterOutputTabs.SuspendLayout();
            this.tabPage3.SuspendLayout();
            this.tabPage4.SuspendLayout();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.Panel2.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            this.SuspendLayout();
            // 
            // JoblistSourceTabs
            // 
            this.JoblistSourceTabs.Controls.Add(this.QueryTabPage);
            this.JoblistSourceTabs.Controls.Add(this.JobListTabPage);
            this.JoblistSourceTabs.Controls.Add(this.DataPackageTabPage);
            this.JoblistSourceTabs.Dock = System.Windows.Forms.DockStyle.Top;
            this.JoblistSourceTabs.Location = new System.Drawing.Point(5, 5);
            this.JoblistSourceTabs.Name = "JoblistSourceTabs";
            this.JoblistSourceTabs.SelectedIndex = 0;
            this.JoblistSourceTabs.Size = new System.Drawing.Size(1093, 130);
            this.JoblistSourceTabs.TabIndex = 3;
            this.JoblistSourceTabs.Tag = "Job_List";
            // 
            // QueryTabPage
            // 
            this.QueryTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.QueryTabPage.Controls.Add(this.jobListPanel1);
            this.QueryTabPage.Location = new System.Drawing.Point(4, 22);
            this.QueryTabPage.Name = "QueryTabPage";
            this.QueryTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.QueryTabPage.Size = new System.Drawing.Size(1085, 104);
            this.QueryTabPage.TabIndex = 0;
            this.QueryTabPage.Tag = "Query";
            this.QueryTabPage.Text = "Query";
            // 
            // jobListPanel1
            // 
            this.jobListPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.jobListPanel1.BaseSQL = "SELECT * FROM V_Mage_Analysis_Jobs ";
            this.jobListPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobListPanel1.Location = new System.Drawing.Point(3, 3);
            this.jobListPanel1.Name = "jobListPanel1";
            this.jobListPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobListPanel1.Size = new System.Drawing.Size(1079, 98);
            this.jobListPanel1.TabIndex = 0;
            // 
            // JobListTabPage
            // 
            this.JobListTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.JobListTabPage.Controls.Add(this.jobIDListPanel1);
            this.JobListTabPage.Location = new System.Drawing.Point(4, 22);
            this.JobListTabPage.Name = "JobListTabPage";
            this.JobListTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.JobListTabPage.Size = new System.Drawing.Size(1085, 104);
            this.JobListTabPage.TabIndex = 1;
            this.JobListTabPage.Tag = "Job_ID_List";
            this.JobListTabPage.Text = "Job List";
            // 
            // jobIDListPanel1
            // 
            this.jobIDListPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.jobIDListPanel1.BaseSQL = "SELECT * FROM V_Mage_Analysis_Jobs ";
            this.jobIDListPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobIDListPanel1.Location = new System.Drawing.Point(3, 3);
            this.jobIDListPanel1.Name = "jobIDListPanel1";
            this.jobIDListPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobIDListPanel1.Size = new System.Drawing.Size(1079, 98);
            this.jobIDListPanel1.TabIndex = 0;
            // 
            // DataPackageTabPage
            // 
            this.DataPackageTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.DataPackageTabPage.Controls.Add(this.jobDataPackagePanel1);
            this.DataPackageTabPage.Location = new System.Drawing.Point(4, 22);
            this.DataPackageTabPage.Name = "DataPackageTabPage";
            this.DataPackageTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.DataPackageTabPage.Size = new System.Drawing.Size(1085, 104);
            this.DataPackageTabPage.TabIndex = 2;
            this.DataPackageTabPage.Tag = "Data_Package";
            this.DataPackageTabPage.Text = "Data Package";
            // 
            // jobDataPackagePanel1
            // 
            this.jobDataPackagePanel1.BaseSQL = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs ";
            this.jobDataPackagePanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobDataPackagePanel1.Location = new System.Drawing.Point(3, 3);
            this.jobDataPackagePanel1.Name = "jobDataPackagePanel1";
            this.jobDataPackagePanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobDataPackagePanel1.Size = new System.Drawing.Size(1079, 98);
            this.jobDataPackagePanel1.TabIndex = 0;
            // 
            // panel1
            // 
            this.panel1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel1.Controls.Add(this.JobListDisplayControl);
            this.panel1.Controls.Add(this.JoblistSourceTabs);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(0, 0);
            this.panel1.Name = "panel1";
            this.panel1.Padding = new System.Windows.Forms.Padding(5);
            this.panel1.Size = new System.Drawing.Size(1105, 320);
            this.panel1.TabIndex = 4;
            // 
            // JobListDisplayControl
            // 
            this.JobListDisplayControl.Accumulator = null;
            this.JobListDisplayControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.JobListDisplayControl.Location = new System.Drawing.Point(5, 135);
            this.JobListDisplayControl.Name = "JobListDisplayControl";
            this.JobListDisplayControl.Notice = "";
            this.JobListDisplayControl.PageTitle = "Title";
            this.JobListDisplayControl.Size = new System.Drawing.Size(1093, 178);
            this.JobListDisplayControl.TabIndex = 0;
            // 
            // panel2
            // 
            this.panel2.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel2.Controls.Add(this.FileListDisplayControl);
            this.panel2.Controls.Add(this.FileSourceTabs);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel2.Location = new System.Drawing.Point(0, 0);
            this.panel2.Name = "panel2";
            this.panel2.Padding = new System.Windows.Forms.Padding(5);
            this.panel2.Size = new System.Drawing.Size(1105, 288);
            this.panel2.TabIndex = 5;
            // 
            // FileListDisplayControl
            // 
            this.FileListDisplayControl.Accumulator = null;
            this.FileListDisplayControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.FileListDisplayControl.Location = new System.Drawing.Point(5, 111);
            this.FileListDisplayControl.Name = "FileListDisplayControl";
            this.FileListDisplayControl.Notice = "";
            this.FileListDisplayControl.PageTitle = "Title";
            this.FileListDisplayControl.Size = new System.Drawing.Size(1093, 170);
            this.FileListDisplayControl.TabIndex = 0;
            // 
            // FileSourceTabs
            // 
            this.FileSourceTabs.Controls.Add(this.tabPage1);
            this.FileSourceTabs.Controls.Add(this.tabPage2);
            this.FileSourceTabs.Dock = System.Windows.Forms.DockStyle.Top;
            this.FileSourceTabs.Location = new System.Drawing.Point(5, 5);
            this.FileSourceTabs.Name = "FileSourceTabs";
            this.FileSourceTabs.SelectedIndex = 0;
            this.FileSourceTabs.Size = new System.Drawing.Size(1093, 106);
            this.FileSourceTabs.TabIndex = 11;
            // 
            // tabPage1
            // 
            this.tabPage1.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage1.Controls.Add(this.jobFilePanel1);
            this.tabPage1.Location = new System.Drawing.Point(4, 22);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage1.Size = new System.Drawing.Size(1085, 80);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Tag = "Job_Files";
            this.tabPage1.Text = "Get Job Files";
            // 
            // jobFilePanel1
            // 
            this.jobFilePanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobFilePanel1.Location = new System.Drawing.Point(3, 3);
            this.jobFilePanel1.Name = "jobFilePanel1";
            this.jobFilePanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobFilePanel1.Size = new System.Drawing.Size(1079, 74);
            this.jobFilePanel1.TabIndex = 10;
            // 
            // tabPage2
            // 
            this.tabPage2.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage2.Controls.Add(this.localFolderPanel1);
            this.tabPage2.Location = new System.Drawing.Point(4, 22);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage2.Size = new System.Drawing.Size(1085, 80);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Tag = "Local_Files";
            this.tabPage2.Text = "Get Local Files";
            // 
            // localFolderPanel1
            // 
            this.localFolderPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.localFolderPanel1.FileNameFilter = "syn.txt";
            this.localFolderPanel1.Folder = "C:\\Data\\syn";
            this.localFolderPanel1.Location = new System.Drawing.Point(3, 3);
            this.localFolderPanel1.Name = "localFolderPanel1";
            this.localFolderPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.localFolderPanel1.Size = new System.Drawing.Size(1079, 74);
            this.localFolderPanel1.TabIndex = 7;
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.fileProcessingPanel1);
            this.panel3.Controls.Add(this.panel4);
            this.panel3.Controls.Add(this.statusPanel1);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel3.Location = new System.Drawing.Point(0, 612);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(1108, 203);
            this.panel3.TabIndex = 6;
            // 
            // panel4
            // 
            this.panel4.Controls.Add(this.FilterOutputTabs);
            this.panel4.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel4.Location = new System.Drawing.Point(0, 0);
            this.panel4.Name = "panel4";
            this.panel4.Padding = new System.Windows.Forms.Padding(5);
            this.panel4.Size = new System.Drawing.Size(1108, 110);
            this.panel4.TabIndex = 16;
            // 
            // FilterOutputTabs
            // 
            this.FilterOutputTabs.Controls.Add(this.tabPage3);
            this.FilterOutputTabs.Controls.Add(this.tabPage4);
            this.FilterOutputTabs.Dock = System.Windows.Forms.DockStyle.Fill;
            this.FilterOutputTabs.Location = new System.Drawing.Point(5, 5);
            this.FilterOutputTabs.Name = "FilterOutputTabs";
            this.FilterOutputTabs.SelectedIndex = 0;
            this.FilterOutputTabs.Size = new System.Drawing.Size(1098, 100);
            this.FilterOutputTabs.TabIndex = 15;
            // 
            // tabPage3
            // 
            this.tabPage3.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage3.Controls.Add(this.folderDestinationPanel1);
            this.tabPage3.Location = new System.Drawing.Point(4, 22);
            this.tabPage3.Name = "tabPage3";
            this.tabPage3.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage3.Size = new System.Drawing.Size(1090, 74);
            this.tabPage3.TabIndex = 0;
            this.tabPage3.Tag = "File_Output";
            this.tabPage3.Text = "File Output";
            // 
            // folderDestinationPanel1
            // 
            this.folderDestinationPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.folderDestinationPanel1.Location = new System.Drawing.Point(3, 3);
            this.folderDestinationPanel1.Name = "folderDestinationPanel1";
            this.folderDestinationPanel1.OutputFolder = "C:\\Data\\Junk";
            this.folderDestinationPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.folderDestinationPanel1.Size = new System.Drawing.Size(1084, 47);
            this.folderDestinationPanel1.TabIndex = 13;
            // 
            // tabPage4
            // 
            this.tabPage4.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage4.Controls.Add(this.sqLiteDestinationPanel1);
            this.tabPage4.Location = new System.Drawing.Point(4, 22);
            this.tabPage4.Name = "tabPage4";
            this.tabPage4.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage4.Size = new System.Drawing.Size(976, 74);
            this.tabPage4.TabIndex = 1;
            this.tabPage4.Tag = "SQLite_Output";
            this.tabPage4.Text = "SQLite Database Output";
            // 
            // sqLiteDestinationPanel1
            // 
            this.sqLiteDestinationPanel1.DatabaseName = "C:\\Data\\test.db";
            this.sqLiteDestinationPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.sqLiteDestinationPanel1.Location = new System.Drawing.Point(3, 3);
            this.sqLiteDestinationPanel1.Name = "sqLiteDestinationPanel1";
            this.sqLiteDestinationPanel1.Size = new System.Drawing.Size(970, 68);
            this.sqLiteDestinationPanel1.TabIndex = 14;
            this.sqLiteDestinationPanel1.TableName = "DMS_Factors";
            // 
            // statusPanel1
            // 
            this.statusPanel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.statusPanel1.Location = new System.Drawing.Point(0, 161);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(1108, 42);
            this.statusPanel1.TabIndex = 1;
            // 
            // fileProcessingPanel1
            // 
            this.fileProcessingPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fileProcessingPanel1.Location = new System.Drawing.Point(0, 110);
            this.fileProcessingPanel1.Name = "fileProcessingPanel1";
            this.fileProcessingPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.fileProcessingPanel1.Size = new System.Drawing.Size(1108, 51);
            this.fileProcessingPanel1.TabIndex = 12;
            // 
            // splitter1
            // 
            this.splitter1.Location = new System.Drawing.Point(0, 0);
            this.splitter1.Name = "splitter1";
            this.splitter1.Size = new System.Drawing.Size(3, 612);
            this.splitter1.TabIndex = 7;
            this.splitter1.TabStop = false;
            // 
            // splitContainer1
            // 
            this.splitContainer1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer1.Location = new System.Drawing.Point(3, 0);
            this.splitContainer1.Name = "splitContainer1";
            this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.panel1);
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.panel2);
            this.splitContainer1.Size = new System.Drawing.Size(1105, 612);
            this.splitContainer1.SplitterDistance = 320;
            this.splitContainer1.TabIndex = 8;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1108, 815);
            this.Controls.Add(this.splitContainer1);
            this.Controls.Add(this.splitter1);
            this.Controls.Add(this.panel3);
            this.Name = "Form1";
            this.Text = "Mage File Processor";
            this.JoblistSourceTabs.ResumeLayout(false);
            this.QueryTabPage.ResumeLayout(false);
            this.JobListTabPage.ResumeLayout(false);
            this.DataPackageTabPage.ResumeLayout(false);
            this.panel1.ResumeLayout(false);
            this.panel2.ResumeLayout(false);
            this.FileSourceTabs.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage2.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.panel4.ResumeLayout(false);
            this.FilterOutputTabs.ResumeLayout(false);
            this.tabPage3.ResumeLayout(false);
            this.tabPage4.ResumeLayout(false);
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel2.ResumeLayout(false);
            this.splitContainer1.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabControl JoblistSourceTabs;
        private System.Windows.Forms.TabPage QueryTabPage;
        private MageClientLib.JobListPanel jobListPanel1;
        private System.Windows.Forms.TabPage JobListTabPage;
        private MageClientLib.JobIDListPanel jobIDListPanel1;
        private System.Windows.Forms.TabPage DataPackageTabPage;
        private MageClientLib.JobDataPackagePanel jobDataPackagePanel1;
        private System.Windows.Forms.Panel panel1;
        private MageClientLib.ListDisplayControl JobListDisplayControl;
        private System.Windows.Forms.Panel panel2;
        private MageClientLib.ListDisplayControl FileListDisplayControl;
        private System.Windows.Forms.Panel panel3;
        private MageClientLib.StatusPanel statusPanel1;
        private MageClientLib.LocalFolderPanel localFolderPanel1;
        private MageClientLib.JobFilePanel jobFilePanel1;
        private System.Windows.Forms.TabControl FileSourceTabs;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.TabPage tabPage2;
        private MageClientLib.FileProcessingPanel fileProcessingPanel1;
        private MageClientLib.FolderDestinationPanel folderDestinationPanel1;
        private MageClientLib.SQLiteDestinationPanel sqLiteDestinationPanel1;
        private System.Windows.Forms.TabControl FilterOutputTabs;
        private System.Windows.Forms.TabPage tabPage3;
        private System.Windows.Forms.TabPage tabPage4;
        private System.Windows.Forms.Panel panel4;
        private System.Windows.Forms.Splitter splitter1;
        private System.Windows.Forms.SplitContainer splitContainer1;
    }
}

