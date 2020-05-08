function Invoke-TervisShopifyPowerShellApplicationProvision {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    Invoke-ApplicationProvision -ApplicationName ShopifyInterface -EnvironmentName $EnvironmentName
    $Nodes = Get-TervisApplicationNode -ApplicationName ShopifyInterface -EnvironmentName $EnvironmentName
    $Nodes | Install-TervisShopifyPowerShellApplicationLog
    $Nodes | Install-TervisShopifyPowerShellApplication_ItemInterface
    $Nodes | Install-TervisShopifyPowerShellApplication_InventoryInterface
    $Nodes | Install-TervisShopifyPowerShellApplication_OrderInterface
    $Nodes | Install-TervisShopifyPowerShellApplication_PersonalizableItemListUpload
    $Nodes | Install-TervisShopifyPowerShellApplication_EndlessAisleItemListUpload
}

function Install-TervisShopifyPowerShellApplicationLog {
    param (
        [Parameter(ValueFromPipelineByPropertyName)]$ComputerName
    )
    begin {
        $LogName = "Shopify"
        $LogSources = `
            "Item Interface",
            "Order Interface",
            "Inventory Interface",
            "Personalizable Item List Upload",
            "Shopify Azure Blob"
    }
    process {
        foreach ($Source in $LogSources) {
            try {
                New-EventLog -ComputerName $ComputerName -LogName $LogName -Source $Source -ErrorAction Stop
            } catch [System.InvalidOperationException] {
                Write-Warning "$Source log already exists."
            }
        }
    }
}

function Install-TervisShopifyPowerShellApplication_ItemInterface {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "TervisPowerShellJobs",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyItemInterface"
            RepetitionIntervalName = "EveryDayEvery15Minutes"
            CommandString = "Invoke-TervisShopifyInterfaceItemUpdate -Environment $EnvironmentName -ScriptRoot `$PowerShellApplicationInstallDirectory"
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters

        $PowerShellApplicationParameters.CommandString = @"
Set-TervisEBSEnvironment -Name $EnvironmentName 2> `$null
Set-TervisShopifyEnvironment -Environment $EnvironmentName
"@
        $PowerShellApplicationParameters.ScriptFileName = "ParallelInitScript.ps1"
        $PowerShellApplicationParameters.Remove("RepetitionIntervalName")
        $PowerShellApplicationParameters.Remove("ScheduledTasksCredential")
        Install-PowerShellApplicationFiles @PowerShellApplicationParameters -ScriptOnly
    }
}

function Install-TervisShopifyPowerShellApplication_InventoryInterface {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "TervisPowerShellJobs",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyInventoryInterface"
            RepetitionIntervalName = "EveryDayAt3am"
            CommandString = "Invoke-TervisShopifyInterfaceInventoryUpdate -Environment $EnvironmentName -ScriptRoot `$PowerShellApplicationInstallDirectory"
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
        
        $PowerShellApplicationParameters.CommandString = @"
Set-TervisEBSEnvironment -Name $EnvironmentName 2> `$null
Set-TervisShopifyEnvironment -Environment $EnvironmentName
"@
        $PowerShellApplicationParameters.ScriptFileName = "ParallelInitScript.ps1"
        $PowerShellApplicationParameters.Remove("RepetitionIntervalName")
        $PowerShellApplicationParameters.Remove("ScheduledTasksCredential")
        Install-PowerShellApplicationFiles @PowerShellApplicationParameters -ScriptOnly

    }
}

function Install-TervisShopifyPowerShellApplication_OrderInterface {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "TervisPowerShellJobs",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyOrderInterface"
            RepetitionIntervalName = "EveryDayEvery15Minutes"
            CommandString = "Invoke-TervisShopifyInterfaceOrderImport -Environment $EnvironmentName"
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
    }
}

function Install-TervisShopifyPowerShellApplication_PersonalizableItemListUpload {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "TervisPowerShellJobs",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyPersonalizedItemListUpload"
            RepetitionIntervalName = "EveryDayAt6am"
            CommandString = @"
Set-TervisEBSEnvironment -Name $EnvironmentName 2> `$null
Invoke-TervisShopifyPersonalizableItemListUpload -Environment $EnvironmentName
"@
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
    }
}

function Install-TervisShopifyPowerShellApplication_EndlessAisleItemListUpload {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {

        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "TervisPowerShellJobs",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyEndlessAisleItemListUpload"
            RepetitionIntervalName = "EveryDayAt6am"
            CommandString = @"
Set-TervisEBSEnvironment -Name $EnvironmentName 2> `$null
Invoke-TervisShopifyEndlessAisleItemListUpload -Environment $EnvironmentName
"@
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
    }
}
