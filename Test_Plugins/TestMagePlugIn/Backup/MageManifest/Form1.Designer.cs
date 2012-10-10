namespace MageManifest {
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
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.panel1 = new System.Windows.Forms.Panel();
            this.listDisplayControl1 = new MageClientLib.ListDisplayControl();
            this.inputFilePanel1 = new MageManifest.InputFilePanel();
            this.textDisplayControl1 = new MageClientLib.TextDisplayControl();
            this.metadataExportPanel1 = new MageClientLib.MetadataExportPanel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.tabControl1.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.tabPage1);
            this.tabControl1.Controls.Add(this.tabPage2);
            this.tabControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl1.Location = new System.Drawing.Point(0, 0);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(823, 579);
            this.tabControl1.TabIndex = 7;
            // 
            // tabPage1
            // 
            this.tabPage1.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage1.Controls.Add(this.listDisplayControl1);
            this.tabPage1.Controls.Add(this.inputFilePanel1);
            this.tabPage1.Location = new System.Drawing.Point(4, 22);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage1.Size = new System.Drawing.Size(815, 553);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Tag = "LVDisplaySource";
            this.tabPage1.Text = "Manifest File";
            // 
            // tabPage2
            // 
            this.tabPage2.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage2.Controls.Add(this.textDisplayControl1);
            this.tabPage2.Location = new System.Drawing.Point(4, 22);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage2.Size = new System.Drawing.Size(747, 553);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Tag = "TextDisplaySource";
            this.tabPage2.Text = "Delimited Text";
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.metadataExportPanel1);
            this.panel1.Controls.Add(this.statusPanel1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel1.Location = new System.Drawing.Point(0, 579);
            this.panel1.Name = "panel1";
            this.panel1.Padding = new System.Windows.Forms.Padding(5);
            this.panel1.Size = new System.Drawing.Size(823, 120);
            this.panel1.TabIndex = 9;
            // 
            // listDisplayControl1
            // 
            this.listDisplayControl1.Accumulator = null;
            this.listDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl1.Location = new System.Drawing.Point(3, 63);
            this.listDisplayControl1.Name = "listDisplayControl1";
            this.listDisplayControl1.Notice = "";
            this.listDisplayControl1.PageTitle = "Title";
            this.listDisplayControl1.Size = new System.Drawing.Size(809, 487);
            this.listDisplayControl1.TabIndex = 2;
            // 
            // inputFilePanel1
            // 
            this.inputFilePanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.inputFilePanel1.FilePath = "C:\\Data\\Manifest.txt";
            this.inputFilePanel1.Location = new System.Drawing.Point(3, 3);
            this.inputFilePanel1.Name = "inputFilePanel1";
            this.inputFilePanel1.Padding = new System.Windows.Forms.Padding(5);
            this.inputFilePanel1.Size = new System.Drawing.Size(809, 60);
            this.inputFilePanel1.TabIndex = 3;
            // 
            // textDisplayControl1
            // 
            this.textDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textDisplayControl1.Location = new System.Drawing.Point(3, 3);
            this.textDisplayControl1.Name = "textDisplayControl1";
            this.textDisplayControl1.Padding = new System.Windows.Forms.Padding(5);
            this.textDisplayControl1.Size = new System.Drawing.Size(741, 547);
            this.textDisplayControl1.TabIndex = 5;
            // 
            // metadataExportPanel1
            // 
            this.metadataExportPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.metadataExportPanel1.Location = new System.Drawing.Point(0, 0);
            this.metadataExportPanel1.Name = "metadataExportPanel1";
            this.metadataExportPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.metadataExportPanel1.Size = new System.Drawing.Size(823, 80);
            this.metadataExportPanel1.TabIndex = 8;
            // 
            // statusPanel1
            // 
            this.statusPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.statusPanel1.Location = new System.Drawing.Point(0, 76);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(823, 42);
            this.statusPanel1.TabIndex = 0;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(823, 699);
            this.Controls.Add(this.tabControl1);
            this.Controls.Add(this.panel1);
            this.Name = "Form1";
            this.Text = "Mage - Metadata From Manifest";
            this.tabControl1.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage2.ResumeLayout(false);
            this.panel1.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private MageClientLib.StatusPanel statusPanel1;
        private MageClientLib.ListDisplayControl listDisplayControl1;
        private InputFilePanel inputFilePanel1;
        private MageClientLib.TextDisplayControl textDisplayControl1;
        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.TabPage tabPage2;
        private MageClientLib.MetadataExportPanel metadataExportPanel1;
        private System.Windows.Forms.Panel panel1;
    }
}

