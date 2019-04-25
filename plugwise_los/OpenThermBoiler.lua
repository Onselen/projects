--[[
file:           OpenThermBoiler.lua
author:         JP Florijn
created:        28-04-2014

\class{OpenThermBoiler}{concrete}{Represents an OpenThermBoiler.}

The OpenThermBoiler. JPF
--]]

local domObject                     = require "util.DomainObject"
local string                        = require "util.string"
local Tif                           = require "rules.thermo.tif"
local StateManager                  = require "rules.thermo.StateManager"
local otValue                       = require "rules.thermo.OpenThermDataValue"
local otFaultCodes                  = require "util.openTherm.faultCodes"
local OTDataValue                   = require "rules.thermo.OpenThermDataValue"
local ObservableMap                 = require "util.table.observable"
local ruleContains                  = require "util.core.GenericRule.contains"
local provideService                = require "util.core.provideService"
local updateService                 = require "util.core.updateService"
local passOnMeasurement             = require "util.core.passOnMeasurement"
local refreshMeasurement            = require "util.core.refreshConsecutiveMeasurement"
local boilerStatus                  = require "util.openTherm.boilerStatus"
local slaveConfiguration            = require "util.openTherm.slaveConfiguration"
local remoteParameterFlags          = require "util.openTherm.remoteParameterFlags"
local dipSwitch                     = require "util.openTherm.elga.dipSwitch"
local array                         = require "util.array"
local table                         = require "util.table"
local equal                         = require "util.equal"
local number                        = require "util.number"
local cloudEventManager             = require "cloudEventManager"
local notificationManager           = require "notificationManager"
local OTPayload                     = require "rules.thermo.OpenThermPayload"
local makeThermoRequest             = require "util.core.proxy.makeThermoRequest"
local config                        = require "config"
local gateway                       = require "gateway"
local eventManager                  = require "eventManager"
local indexByValue                  = require "util.table.indexByValue"
local boilerUtil                    = require "util.openTherm.boilerUtil"
local tagCache                      = require "util.core.tagCache"
-- Setup localization.
local localization                  = require "localization"
localization.loadYamlsFromDirectory(config.localizationDirectory)

local State                         = require "DomainObject.Service.Meter.PointMeter.State"
local onOffClasses                  = State:subClasses()
local factory, class                = require("DomainObject.Protocol.Boiler"):extend("OpenThermBoiler", false)

local otVendorNames                 = require("config")["ot-vendor-names"]
local otSetpointStrategies          = require("config")["ot-setpoint-strategies"]
local otLogConf                     = require("config")["open-therm-logging"]
local settings                      = require("config")["thermo-settings"]
local daemonConfig                  = require("config")["daemon-config"]

local fixedRoomSetpointStrategy     = gateway.core["room-setpoint-strategy"]
local dest                          = (gateway.gateway.firmware.type == "smile_open_therm" and  Tif.SourceDest.OpenThermSlave)
                                    or                                                          Tif.SourceDest.CentralHeater

-- Forward declaration.
local tspVocabulary                 = require("config")["open-therm-slave-parameters"]
local genericTspTable               = {polltervals = {SP = {}}, repeated_calls = {}, events = {},}
local MAX_INT16 = 65535 -- 2^31 - 1

-- TODO: alwaysUnknown: These need special handling. Disable for now.
local variableInfo = {
    control_setpoint                            = { alwaysAck     = true, retryIndefinitely = true, masterLeading = true,   }, -- OT Id:   1
    master_configuration                        = { alwaysAck     = true, acceptWithoutPassthrough                = true,   }, -- OT Id:   2
    remote_request                              = { alwaysUnknown = true,                                                   }, -- OT Id:   4
    cooling_modulation_level                    = { alwaysAck     = true, retryIndefinitely = true, masterLeading = true,   }, -- OT Id:   7
    maximum_relative_modulation_level           = { alwaysAck     = true, retryIndefinitely = true, masterLeading = true,   }, -- OT Id:  14
    room_setpoint                               = { alwaysAck     = true,                           masterLeading = true,   }, -- OT Id:  16
    week_time                                   = { alwaysAck     = true,                           masterLeading = true,   }, -- OT Id:  20
    date                                        = { alwaysAck     = true,                           masterLeading = true,   }, -- OT Id:  21
    year                                        = { alwaysAck     = true,                           masterLeading = true,   }, -- OT Id:  22
    room_temperature                            = { alwaysAck     = true,                           masterLeading = true,   }, -- OT Id:  24
    outside_temperature                         = { alwaysAck     = true,                                                   }, -- OT Id:  27
    humidity                                    = { alwaysAck     = true,                                                   }, -- OT Id:  38
    domestic_hot_water_setpoint                 = {                                                                         }, -- OT Id:  56,
    maximum_boiler_setpoint                     = {                                                                         }, -- OT Id:  57,
    relative_ventilation_postition              = { alwaysAck     = true, retryIndefinitely = true, masterLeading = true,   }, -- OT Id:  71
    exhaust_air_humidity                        = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id:  78
    exhaust_air_carbon_dioxide_level            = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id:  79
    vhr_slave_param_index                       = { alwaysUnknown = true,                                                   }, -- OT Id:  89
    sensor_info                                 = { alwaysUnknown = true,                                                   }, -- OT Id:  98
    remote_override_operating_mode              = { alwaysUnknown = true,                                                   }, -- OT Id:  99
    solar_storage_param_index                   = { alwaysUnknown = true,                                                   }, -- OT Id: 106
    electricity_production_starts               = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 109
    electricity_production_hours                = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 110
    cumulative_electricity_production           = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 112
    unsuccessful_burner_starts                  = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 113
    flame_signal_too_low_number                 = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 114
    successful_burner_starts                    = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 116
    central_heater_pump_starts                  = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 117
    domestic_hot_water_pump_starts              = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 118
    domestic_hot_water_burner_starts            = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 119
    burner_operation_hours                      = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 120
    central_heater_pump_operation_hours         = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 121
    domestic_hot_water_pump_operation_hours     = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 122
    domestic_hot_water_burner_operation_hours   = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 123
    open_therm_version_master                   = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 124
    master_version                              = { alwaysAck     = true, retryIndefinitely = true,                         }, -- OT Id: 126
}

local openThermTimeout = settings.timeout.OS

local smartgridControlEnum = {
    smart_grid_off          = 0,
    default                 = 1,
    heating_cooling_only    = 2,
}

local testModeEnum = {
    off                     = 0,
    circulation             = 1,
    circulation_heat        = 2,
    circulation_heat_boiler = 3,
    circulation_cooling     = 4,
}

local function specialRequest(method, destination, varName)
    return {
        method                      = "ReadData",
        destination                 = "OS",
        ["slave_parameters"]        = {varName, 0},
    }
end

local function specialThermoRequest(method, destination, varName)
    return {
        method                      = "ReadData",
        destination                 = "CH",
        ["slave_parameters"]        = {varName, 0},
    }
end

local function handleOtFault(self)
    local otFaults      = factory:getTemp(self).variables.otFaultCodes

    local asFaultCodes  = otFaultCodes.applicationSpecificFaultCodes
    local faults        = otFaultCodes.map(asFaultCodes, otFaults[1] or 0)

    local oemFaultCodes = (self.module.vendorName == "Techneco" and otFaultCodes.technecoFaultCodes) or {}
    otFaultCodes.map(oemFaultCodes, otFaults[2] or 0, faults)

    for fault, active in pairs(faults) do
        local isOEMFault        = not asFaultCodes[fault]
        local kind              = string.format("otfault:%s", fault)
        local faultType         = ((fault == "lockout_reset" or fault == "service_required") and "message") or "error"
        local notifsOfThisKind  = array.filter(notificationManager.filter.ofKind(kind), notificationManager.activeNotifications())
        local issueCloudEvent   = false
        if active then
            if #notifsOfThisKind == 0 then                                      -- None of this kind yet, so issue
                dbgf(5,"we have to issue a `%s' notification since otApplicationSpecificFault is `%s'", kind, tostring(fault) or "<nil>")
                notificationManager.issueNotification(  faultType,                                      -- type
                                                        kind,                                           -- kind (yes, I know, sorry...)
                                                        localization.get("OpenTherm", "error", fault),  -- title
                                                        localization.get("OpenTherm", "id5", fault))    -- message
                issueCloudEvent = not isOEMFault
            else                                                                -- Some of this kind, so update localization etc.
                notificationManager.updateNotifications(notifsOfThisKind,
                                                        faultType,                                      -- type
                                                        kind,                                           -- kind (still sorry...)
                                                        localization.get("OpenTherm", "error", fault),  -- title
                                                        localization.get("OpenTherm", "id5", fault))    -- message
            end
        else                                                                    -- not active, so revoke
            issueCloudEvent = (#notifsOfThisKind > 0) and not isOEMFault
            notificationManager.revokeNotifications(notifsOfThisKind)
        end

        if issueCloudEvent then
            cloudEventManager.event(string.format("%s_%s",
                                                    ((faultType == "error" or faultType == "warning")   and faultType) or "info",
                                                    (active                                             and "occured") or "resolved"),
                                    fault,
                                    scheduler.time())
        end
    end
    local ext = self.thermoExtension
    if ext and ext.thermoTouch and self.module.vendorName == "Techneco" then
        local oemFault = otFaults[2]
        -- Don't actually require rules.thermo.Display; we do not wish to compile Smile T tooling into Smile OT firmwares.
        local Display = package.loaded["rules.thermo.Display"]
        if oemFault > 0 then
            Display.setBackground("default", "error")
            Display.setText('default', 'oem_fault', string.format("OEM fault: %i", oemFault))
        else
            Display.setBackground("default", "normal")
            Display.setText('default', 'oem_fault', '')
        end
        local temp = factory:getTemp(self)
        if oemFault ~= temp.lastOEMFault then
            Display.setBacklight(true)
        end
        temp.lastOEMFault = oemFault
    end
end

local function handleCentralHeatingState(self, temp, interface, CHOn, relModLevel)
    if not self.cooling then
        -- The Techneco Elga often pretends to be off, but its relative_modulation_level shows its true colors.
        local heatpumpOn = (self.module.vendorName == "Techneco") and (relModLevel or temp.variables.relativeModulationLevel or -1) > 0
        if self.thermoExtension and self.thermoExtension.thermoTouch then
            -- Don't actually require rules.thermo.Display; we do not wish to compile Smile T tooling into Smile OT firmwares.
            local Display = package.loaded["rules.thermo.Display"]
            if heatpumpOn then
                Display.showIcon("default", "cooling",  false)
                Display.showIcon("default", "burning",  false)
                Display.showIcon("default", "heatpump", true)
            elseif CHOn then
                Display.showIcon("default", "cooling",  false)
                Display.showIcon("default", "heatpump", false)
                Display.showIcon("default", "burning",  true)
            else
                Display.showIcon("default", "cooling",  false)
                Display.showIcon("default", "heatpump", false)
                Display.showIcon("default", "burning",  false)
            end
        end

        if (not CHOn) and heatpumpOn then
            dbg(3, "Changed CHOn to true, because the Elga is active.")
            CHOn = true
        end
    end
    -- Setting boiler_state explicitly triggers the burner efficiency determination algorithm found in the parent class.
    interface:setHeatingState(scheduler.time(), (CHOn and "on") or "off")
end

local function logTspVariable(self, logConf, varName, val, vendor)
    local temp      = factory:getTemp(self)
    local interface = factory:getInterface(self)
    boilerUtil.logOpenThermVariable(self, temp, interface, logConf, varName, val, tspVocabulary.vendors[vendor].variables[varName])
end


local function tspHandler(self, tspStateTable, index, val, old, vName, key)
    local logC, snakey
    self.tsp[index] = val
    if self.intendedTsp[index] then
        self.intendedTsp[index] = false
    end
    if tspStateTable.modifiedPollterval then
        tspStateTable.modifiedPollterval[index] = nil
    end
    local definition = tspVocabulary.vendors[vName]
    if definition then
        snakey = string.toSnakeCase(key)
        logC = definition.logging[snakey]
        local map
        if not equal(val, old) then
            dbgf(3, "Received update for variable TSP.%s: %s -> %s", snakey, tostring(old), tostring(val))
        end
        if index == definition.tspTestModeIndex then
            self.testMode = indexByValue(testModeEnum, val)
            return
        elseif index == definition.tspSmartgridControlIndex then
            self.smartgridControl = indexByValue(smartgridControlEnum, val)
            return
        elseif index == definition.tspDipSwitchAIndex or index == definition.tspDipSwitchBIndex then
            map = dipSwitch.decode(val, nil, (index == definition.tspDipSwitchAIndex and "a") or "b")
            for key, val in pairs(map) do
                dbgf(3, "Setting dipSwitch parameter: %s:=%s", key, val)
                logTspVariable(self, logC[string.toSnakeCase(key)], key, val, vName)
            end
            return
        end
        assert(tspVocabulary.vendors[vName].variables[snakey], string.format("%s is not specified in the TSP interface", snakey))
    end


    if logC then
        logTspVariable(self, logC, snakey, val, vName)
    elseif not tspStateTable.variables[key] then
        dbg(3, "No handler or log defined for TSP variable '", snakey, "'.")
    end

end

local function tspGuard(self, tspStateTable, index, val, command)
    -- Temporary behaviour, to be replaced by proper checks.
    local allowed, old, vName, definition, key = true
    if self.tspSupported then
        old         = self.tsp[index]
        vName       = self.module.vendorName
        definition  = tspVocabulary.vendors[vName]
        if definition then
            for iName, var in pairs(definition.variables) do
                if var.id == index then
                    key = iName
                end
            end
            if not (key) then
                return "DataInvalid"
            end
            if definition.variables[key].access == "RO" then
                allowed = false
            elseif definition.logging[key] then
                local min, max, ignored = definition.logging[key]['minimum-value'], definition.logging[key]['maximum-value'], definition.logging[key]['ignored-value']
                if (min and (val < min)) or (max and (val > max)) or (ignored and (equal(val, ignored, 0.01))) then
                    allowed = false
                end
            end
        end
        if command == "WriteData" then
            if allowed then
                self.intendedTsp[index] = val;
                ((self.thermoExtension and proxies.thermo) or proxies.otgateway).queueRequest(makeThermoRequest(3, openThermTimeout, {
                    method              = "WriteData",
                    destination         = (self.thermoExtension and "CH") or "OS",
                    slave_parameters    = {index, val}
                }))
                return "WriteAck"
            else
                return "DataInvalid"
            end
        elseif command == "ReadData" then
            if self.tsp[index] then
                return "ReadAck"
            else
                return "DataInvalid"
            end
        elseif command:match("Ack$") then
            tspHandler(self, tspStateTable, index, val, old, vName, key)
            return
        end
    else
        return "UnknownDataId"
    end
end

local function evaluateCooling(self, temp)
    local interface = factory:getInterface(self)
    local ext = self.thermoExtension
    if ext then
        if self.module.vendorName == "Techneco" then
            local cooling = false
            local activationTemp, deactivationThreshold = self.coolingActivationOutdoorTemperature, self.coolingDeactivationThreshold
            local outdoorTemp                           = temp.variables.outsideTemperature
            local outdoorTempDate                       = (temp.varUpdated.outside_temperature or config.noDate)
            -- Fall back to WeatherFeed if the sensor's outdoor temperature is older than 3 hours.
            if (not outdoorTemp) or scheduler.time() - outdoorTempDate > 10800 then
                for id, weatherFeed in pairs(objects.WeatherFeed or {}) do
                    if weatherFeed.deletedDate == config.noDate then
                        local outdoorTempSerf   = weatherFeed.module:iterateServices("ThermoMeter", "outdoor_temperature")()
                        local outdoorTempLog    = outdoorTempSerf:iterateFunctionalities("PointLogFunctionality")()
                        if (not outdoorTemp) or (outdoorTempLog.updatedDate or config.noDate) > outdoorTempDate then
                            local temp = select(2, outdoorTempLog.public:last())
                            if temp then
                                outdoorTempDate, outdoorTemp = outdoorTempLog.updatedDate, temp
                            end
                        end
                    end
                end
            end
            if activationTemp and deactivationThreshold and outdoorTemp then
                dbgf(3, "We have an outdoor temperature of %4.2f, a cooling activation temperature of %4.2f, and a deactivation threshold of  %4.2f.", outdoorTemp, activationTemp, deactivationThreshold)
                if self.cooling then
                    -- Disable if under the deactivation threshold.
                    cooling = outdoorTemp >= activationTemp - deactivationThreshold
                    if not cooling then
                        dbgf(3, "Disable cooling because %4.2f is less than %4.2f - %4.2f.", outdoorTemp, activationTemp, deactivationThreshold)
                    end
                elseif outdoorTemp >= activationTemp then
                    -- Enable if over the activation temperature
                    cooling = true
                    dbgf(3, "Enable cooling because %4.2f is greater than or equal to %4.2f.", outdoorTemp, activationTemp)
                end
                if self.cooling == cooling then
                    dbgf(3, "Keep cooling %sabled.", (cooling and "en") or "dis")
                end
            else
                sdbg(3, "Stop cooling for lack of parameters;", outdoorTemp, activationTemp, deactivationThreshold)
            end
            local curCoolingDemand = self.coolingDemand or false
            local touch = ext.thermoTouch
            local coolingDemand = false
            if touch and cooling then
                local tMod          = touch.module
                local thermostat    = tMod:iterateServices("Thermostat",    "thermostat")()
                local thermoMeter   = tMod:iterateServices("ThermoMeter",   "temperature")()
                local thermostatFun = thermostat    and thermostat:iterateFunctionalities("ThermostatFunctionality")()
                local thermoLog     = thermoMeter   and thermoMeter:iterateFunctionalities("PointLogFunctionality")()
                if thermostatFun and thermoLog then
                    local setpoint      = thermostatFun.setpoint
                    local temperature   = select(2, thermoLog.public:last())
                    coolingDemand       = setpoint and temperature and setpoint <= temperature
                    dbgf(3, "At a setpoint of %s and a temperature of %s, we have %scooling demand", setpoint, temperature, (coolingDemand and "") or "no ")
                end
            end
            self.coolingDemand  = coolingDemand
            ext:switchCoolingState((cooling and "on") or "off")
            self.cooling = self.cooling or cooling
            if self.cooling and self.coolingDemand ~= curCoolingDemand then
                interface:sendMSS(2)
            end
            self.cooling        = cooling
        elseif self.cooling then
            dbg(3, "Switch off cooling because we're not connected to an Elga.")
            ext:switchCoolingState("off")
            self.cooling        = false
            self.coolingDemand  = false
        end
    end
end

local function normalizeBounds(bounds, min, max)
    bounds[2] = bounds[2] or min
    bounds[1] = bounds[1] or max
    return bounds
end

local function inlineTostring(val)
    if type(val) == "table" then
        local t = {'{'}
        for k, v in pairs(val) do
            table.insert(t, string.format("%s = %s, ", tostring(k), tostring(v)))
        end
        table.insert(t, "}")
        return table.concat(t)
    end
    return tostring(val)
end

local function processRemoteParameterFlags(self)
    local temp, interface = factory:getTemp(self), factory:getInterface(self)
    if self.DHWWritable then
        local bounds = temp.variables.domesticHotWaterBounds
        if bounds then
            boilerUtil.provideThermostat(   self, interface, "domestic_hot_water_setpoint", "C", (bounds[2] > 0 and bounds[2]) or 30, (bounds[1] > 0 and bounds[1]) or 60, 0.01)
        end
    elseif self.DHWReadable then
        boilerUtil.provideThermoMeter(      self, interface, "domestic_hot_water_setpoint", "C")
    end
    if self.maxCHSetpointWritable then
        local bounds = temp.variables.boilerSetpointBounds
        if bounds then
            boilerUtil.provideThermostat(   self, interface, "maximum_boiler_temperature", "C",  (bounds[2] > 0 and bounds[2]) or 30, (bounds[1] > 0 and bounds[1]) or 80, 0.01)
        end
    elseif self.maxCHSetpointReadable then
        boilerUtil.provideThermoMeter(      self, interface, "maximum_boiler_temperature", "C")
    end
end


local function defineVariableObservers(self)
    local temp, interface = factory:getTemp(self), factory:getInterface(self)
    return {
        ["*"] = function(val, key, old)
            local snakey = string.toSnakeCase(key)

            if not equal(val, old) then
                dbgf(3, "Received update for variable CH.%s: %s -> %s", snakey, inlineTostring(old), inlineTostring(val))
            end

            local logC = otLogConf[snakey]

            if          (snakey == "maximum_boiler_setpoint"        and self.maxCHSetpointReadable and not self.maxCHSetpointWritable)
                    or  (snakey == "domestic_hot_water_setpoint"    and self.DHWReadable and not self.DHWWritable) then
                logC = table.copy(logC)
                logC['service-class'] = "ThermoMeter"
                dbgf(3, "Use a ThermoMeter, not a Thermostat for variable %s.", snakey)
                boilerUtil.provideThermoMeter(self, interface, snakey, "C")
            end

            assert(Tif.variables[dest][snakey], string.format("%s is not specified in the interface", snakey))
            local type = Tif.variables[dest][snakey] and Tif.variables[dest][snakey].type

            if logC and logC ~= "" then
                if "2uint8" == type then
                    if next(logC[1]) then boilerUtil.logOpenThermVariable(self, temp, interface, logC[1], snakey, val[1]) end
                    if next(logC[2]) then boilerUtil.logOpenThermVariable(self, temp, interface, logC[2], snakey, val[2]) end
                else
                    if          self.openThermGateway
                            and (   logC['service-class'] == 'CumulativeCounter'
                                or  logC['service-class'] == 'CumulativeChronoMeter') then
                        if val >= MAX_INT16 then
                            dbgf(3, "Reset variable %s to 0, because it reached the maximum value of %s.", key, tostring(val))
                            proxies.otgateway.queueRequest(makeThermoRequest(3, openThermTimeout, {
                                method              = "WriteData",
                                destination         = "OS",
                                [snakey]            = 0,
                            }))
                        elseif val == 0 and val < (old or 0) then
                            self.offset[snakey] = old + ((self.offset[snakey] and self.offset[snakey]) or 0)
                            dbgf(3, "Variable %s was reset to 0; was previously %s. From now on, we will add an offset of %s to any new values.", key, tostring(old), tostring(self.offset[snakey]))
                        end
                    end

                    boilerUtil.logOpenThermVariable(self, temp, interface, logC, snakey, val)
                end
            elseif not temp.variables[key] then
                dbg(3, "No handler or log defined for OpenTherm variable '", snakey, "'.")
            end
        end,
        ["slaveParameters"] = function(val, key, old)
            tspGuard(self, temp.tspTable, val[1], val[2], "Ack")
        end,
        ["slaveParameterNumber"] = function(val, key, old)
            --If change then request all slave paramters and store. Save also unkD and InvDat
            old = old or {[1] = 0}
            if val[1] ~= old[1] then
                for tspKey in pairs(temp.tspTable.varUpdated) do
                    temp.tspTable.varUpdated[tspKey] = nil
                end
            end
            if val[1] > 0 then
                self.tspSupported = true
                -- Only do generic TSP polling if we're a Smile OT and might need to pass through TSPs to the Thermostat.
                if self.openThermGateway and not tspVocabulary.vendors[self.module.vendorName] then
                    -- Generate polltervals for the StateManager
                    for i=0,val[1],1 do
                        genericTspTable.polltervals.SP[i] = 3600
                    end
                end
            end
            if val[1] == 0 and old[1] ~= val[1] then
                self.tspSupported = false
                for i=0,old[1],1 do
                    genericTspTable.polltervals.SP[i] = nil
                end
            end
        end,
        ["slaveConfiguration"] = function(val, key, old)
            slaveConfiguration.decode(val[1], self)
            dbg(3, "Updated boiler configuration:\n", slaveConfiguration.toString(self))
            local oldVendorName = self.module.vendorName
            local vendorName
            if gateway.core["override-boiler-vendor-name"] then
                vendorName = gateway.core["override-boiler-vendor-name"]
                dbg(3, "The OpenTherm member ID of this boiler is overridden by a hardcoded default: ", vendorName)
            else
                local elgaSupport = self.module.gateway:iterateFeatures("ElgaSupport")()
                local elgaEnabled = (elgaSupport and elgaSupport:isValid() and true) or false
                if elgaEnabled then
                    dbg(3, "The OpenTherm member ID of this boiler is set to Techneco because there is an active ElgaSupport Feature on the Gateway.")
                    vendorName = "Techneco"
                else
                    -- Low byte is the OT member ID of the boiler.
                    vendorName = otVendorNames[tonumber(val[2])] or "Unknown"
                end
            end

            if vendorName ~= oldVendorName then
                dbg(3, "Set the boiler vendorName to ", vendorName)
                self.module.vendorName = vendorName

                for index, val in pairs(self.tsp) do
                    if val then
                        tspGuard(self, temp.tspTable, index, val, "Ack")
                    end
                end
            end

            if vendorName == "Techneco" then
                -- Setup cooling activation temperature and deadband.
                local ext   = self.thermoExtension
                local tt    = ext and ext.thermoTouch
                if tt then
                    local module = tt.module
                    if not module:iterateServices("Thermostat", "cooling_activation_outdoor_temperature")() then
                        provideService(     module, nil,        "Thermostat",   "cooling_activation_outdoor_temperature",   "C", 1, 40, 0.1):newConsecutiveMeasurement(nil, scheduler.time(), 40)
                        provideService(     module, nil,        "Threshold",    "cooling_deactivation_threshold",           "C", 1, 40, 0.1):newConsecutiveMeasurement(nil, scheduler.time(),  4)
                    end
                end
                -- Poll outside_temperature more often for Elga.
                temp.modifiedPollterval.outside_temperature = math.min(30, settings.polltervals[ext and "CH" or "OS"].outside_temperature or math.huge)

                if vendorName ~= oldVendorName and domObject.isAn("AMERegulation", self.regulation) then
                    self.regulation.openThermBoilerActivationThreshold  = 0
                    self.regulation.gradualAdHocHeatingRate             = 99
                    self.correctedElgaActivationThreshold               = os.time()
                end
            end
        end,
        ["masterConfiguration"] = function(val, key, old)
            local ott = self.openThermGateway and self.openThermGateway.openThermThermostat
            if ott then
                ott.module.vendorName = otVendorNames[tonumber(val[2])] or "Unknown"
                dbg(3, "Set the thermostat vendorName to ", ott.module.vendorName)
            end
        end,
        ["relativeModulationLevel"] = function(val, key, old)
            if self.module.vendorName == "Techneco" then
                -- The Techneco Elga often pretends to be off, but its relative_modulation_level shows its true colors.
                handleCentralHeatingState(self, temp, interface, temp.boilerStatus.slave.CHOn, val)
            end
        end,
        ["outsideTemperature"] = function(val, key, old)
            local ext   = self.thermoExtension
            if ext then
                local touch = ext and ext.thermoTouch
                if touch then
                    touch:setOutdoorsTemperature(interface, val)
                end
                evaluateCooling(self, temp)
            end
        end,
        ["remoteParameterFlags"] = function(val, key, old)
            remoteParameterFlags.decode(val, self)
            dbg(3, "Updated remote parameter flags:\n", remoteParameterFlags.toString(self))
            processRemoteParameterFlags(self)
            interface:sendRemoteParameter('remote_parameter_flags', remoteParameterFlags.encode({DHWReadable = self.DHWReadable, maxCHSetpointReadable = self.maxCHSetpointReadable}))
        end,
        ["domesticHotWaterBounds"] = function(val, key, old)
            local bounds = normalizeBounds(temp.variables.domesticHotWaterBounds, 30, 60)
            if bounds[1] < bounds[2] then
                dbgf(3, "Ignoring nonsense DHW bounds (lower: %s, upper: %s).", tostring(bounds[2]), tostring(bounds[1]))
                temp.variables:setUnderhand(key, old)
                bounds = temp.variables.domesticHotWaterBounds or {}
                dbgf(3, "Reverted DHW bounds (lower: %s, upper: %s).", tostring(bounds[2]), tostring(bounds[1]))
            elseif self.DHWWritable then
                boilerUtil.provideThermostat(self, interface, "domestic_hot_water_setpoint",    "C", number.constrain(25, 100, bounds[2] or 30), number.constrain(25, 100, bounds[1] or 60), 0.01)
            end
        end,
        ["boilerSetpointBounds"] = function(val, key, old)
            local bounds = normalizeBounds(temp.variables.boilerSetpointBounds, 30, 80)
            if bounds[1] < bounds[2] then
                dbgf(3, "Ignoring nonsense max CH bounds (lower: %s, upper: %s).", tostring(bounds[2]), tostring(bounds[1]))
                temp.variables:setUnderhand(key, old)
                bounds = temp.variables.boilerSetpointBounds or {}
                dbgf(3, "Reverted max CH bounds (lower: %s, upper: %s).", tostring(bounds[2]), tostring(bounds[1]))
            elseif self.maxCHSetpointWritable then
                boilerUtil.provideThermostat(self, interface, "maximum_boiler_temperature",     "C",  number.constrain(25, 100, bounds[2] or 25), number.constrain(25, 100, bounds[1] or 80), 0.01)
            end
        end,
        ["boilerCapacityInformation"] = function(val, key, old)
            if val[1] > 0 then self.maximumCapacity = val[1] end
            self.minimumModulationLevel = val[2]
        end,
        ["otFaultCodes"] = function(val, key, old)
            handleOtFault(self)
        end,
        ["openThermVersionSlave"] = function(val, key, old)
            self.openThermVersion = string.format("%.3f", val):gsub("(%.[0-9][1-9]*)0+$", "%1")
        end,
        ["openThermVersionMaster"] = function(val, key, old)
            local ott = self.openThermGateway and self.openThermGateway.openThermThermostat
            if ott then
                ott.openThermVersion = string.format("%.3f", val):gsub("(%.[0-9][1-9]*)0+$", "%1")
            end
        end,
        ["slaveVersion"] = function(val, key, old)
            self.module.vendorModel = string.format("%i.%i", unpack(val))
        end,
        ["masterVersion"] = function(val, key, old)
            local ott = self.openThermGateway and self.openThermGateway.openThermThermostat
            if ott then
                ott.module.vendorModel = string.format("%i.%i", unpack(val))
            end
        end,
    }
end

-- Watch the OpenThermMaster status, the high byte of OT ID 0.
local function defineMasterStatusObservers(self)
    local temp, interface = factory:getTemp(self), factory:getInterface(self)
    return {
        ["*"] = function(val, key, old)
            if val ~= old then
                dbgf(3, "Received update for variable CH.boilerStatus.master.%s: %s -> %s", key, tostring(old), tostring(val))
            end
            local logC = otLogConf.master_status[key]
            if logC then
                boilerUtil.logOpenThermVariable(self, temp, interface, logC, key, (val and "on") or "off")
            end
        end,
        ["CHEnabled"] = function(val, key, old)
            interface:setIntendedHeatingState(scheduler.time(), (val and "on") or "off")
        end,
        ["DHWEnabled"] = function(val, key, old)
            passOnMeasurement(self.module, factory:getTemp(self), nil, "DomesticHotWaterToggle", "domestic_hot_water_comfort_mode", nil, nil, (val and "on") or "off", "")
        end,
    }
end

-- Watch the OpenThermSlave status, the high byte of OT ID 0.
local function defineSlaveStatusObservers(self)
    local Display = package.loaded["rules.thermo.Display"]
    local temp, interface = factory:getTemp(self), factory:getInterface(self)
    return {
        ["*"] = function(val, key, old)
            if val ~= old then
                dbgf(3, "Received update for variable CH.boilerStatus.slave.%s: %s -> %s", key, tostring(old), tostring(val))
            end
            local logC = otLogConf.slave_status[key]
            if logC then
                boilerUtil.logOpenThermVariable(self, temp, interface, logC, key, (val and "on") or "off")
            end
        end,
        ["fault"] = function(val, key, old)
            local kind = "otval"
            local notifsOfThisKind = array.filter(notificationManager.filter.ofKind(kind), notificationManager.activeNotifications())
            if val and #notifsOfThisKind == 0 then
                dbg(5, "Issue a `slave_fault' notification")
                notificationManager.issueNotification(  "error",    -- type
                                                        kind,       -- kind (yes, I know, sorry...)
                                                        localization.get("OpenTherm", "slave_fault", "title"),
                                                        localization.get("OpenTherm", "slave_fault", "message"))
                cloudEventManager.event("error_occured", "slave_fault", scheduler.time())
            else
                notificationManager.revokeNotifications(notifsOfThisKind) -- not active, remove if any

                if #notifsOfThisKind > 0 then
                    dbg(5, "Revoke a `slave_fault' notification")
                    cloudEventManager.event("error_resolved", "slave_fault", scheduler.time())
                end
            end
        end,
        ["CHOn"] = function(val, key, old)
            handleCentralHeatingState(self, temp, interface, val)
            local ext = self.thermoExtension
            -- Polltervals are sufficiently aggressive for direct OT communication; but if indirect, increase if necessary.
            if ext and val ~= old then
                if      true == val then
                    temp.modifiedPollterval.boiler_fan_speed                    = 10
                    temp.modifiedPollterval.boiler_heat_exchanger_temperature   = 10
                    temp.modifiedPollterval.exhaust_temperature                 = 10
                    scheduler:resume(temp.pollThread)
                elseif  false == val then
                    temp.modifiedPollterval.boiler_fan_speed                    = nil
                    temp.modifiedPollterval.boiler_heat_exchanger_temperature   = nil
                    temp.modifiedPollterval.exhaust_temperature                 = nil
                end
            end
        end,
        ["DHWOn"] = function(val, key, old)
            -- Polltervals are sufficiently aggressive for direct OT communication; but if indirect, increase if necessary.
            if val ~= old and self.thermoExtension then
                if      true == val then
                    temp.modifiedPollterval.domestic_hot_water_flow_rate        = 10
                    scheduler:resume(temp.pollThread)
                elseif false == val then
                    temp.modifiedPollterval.domestic_hot_water_flow_rate        = nil
                end
            end
            if Display and self.thermoExtension and self.thermoExtension.thermoTouch then
                Display.showIcon("default", "faucet",   val)
            end
        end,
        ["cooling"] = function(val, key, old)
            if Display and self.thermoExtension and self.thermoExtension.thermoTouch then
                if val then
                    Display.showIcon("default", "heatpump", false)
                    Display.showIcon("default", "burning",  false)
                end
                Display.showIcon("default", "cooling",  val)
            end
            if "Techneco" == self.module.vendorName then
                passOnMeasurement(self.module, temp, nil, "BoilerState", "cooling_state", nil, nil, (val and "on") or "off", "")
            end
        end,
    }
end

local function isAcceptableOTVariable(snakey, value)
    -- Make sure variable exists, value is parsable and bounds are not swapped.
    local definition = Tif.variables[dest][snakey]
    return  definition
        and OTDataValue.isParsable(value, definition.type)
        and (   (not snakey:match('bounds$'))
            or  value[1] >= value[2])
end


local function refreshMeasurementForLogConf(self, temp, logConf, updatedTime)
    local interface = factory:getInterface(self)
    local module = interface:getModuleForSide(logConf.side)
    if module then
        return refreshMeasurement(  module,
                                    (logConf.side == "OpenThermBoiler" and temp) or nil,
                                    logConf['service-class'],
                                    logConf.type,
                                    updatedTime)
    end
end

-- This product version is incremented when we implement new features exposed over OpenTherm.
local otProductVersion = 1

local function createPollWorker(self)
    local temp, interface = factory:getTemp(self), factory:getInterface(self)
    if temp.pollThread then return end
    local function pollWorker()
        while true do
            while self.deletedDate ~= config.noDate or not temp.isInitialized() do
                dbg(5,"not initialized")
                scheduler:suspend(1)
            end

            local now = scheduler.time()

            if now > (temp.lastVariableRefresh or 0) + 300 then
                local snakey, logConf, recordedTime
                for cakey, updatedTime in pairs(temp.varUpdated) do
                    snakey          = string.toSnakeCase(cakey)
                    logConf         = otLogConf[snakey]
                    recordedTime    = temp.varUpdateRecorded[snakey]

                    if logConf and recordedTime and updatedTime > recordedTime then
                        temp.varUpdateRecorded[snakey] = updatedTime



                        if (not logConf[1]) and (not logConf[2]) then
                            refreshMeasurementForLogConf(self, temp, logConf, updatedTime)
                        end

                        if logConf[1] then
                            refreshMeasurementForLogConf(self, temp, logConf[1], updatedTime)
                        end

                        if logConf[2] then
                            refreshMeasurementForLogConf(self, temp, logConf[2], updatedTime)
                        end
                    end
                end
                temp.lastVariableRefresh = now
            end

            if now - (temp.exchangedVersionInfo or 0) > 600 then
                temp.exchangedVersionInfo = now
                local prx = proxies.otgateway
                -- Set OS.master_product_type, OS.master_product_version. Of the former, 143 is the Smile T, 159 the Smile OT.
                if self.thermoExtension then
                    prx = proxies.thermo
                    prx.queueRequest(makeThermoRequest(4, openThermTimeout, {
                        method                      = "WriteData",
                        destination                 = "CH",
                        master_configuration        = {0, 140},
                    }))
                    prx.queueRequest(makeThermoRequest(4, openThermTimeout, {
                        method                      = "WriteData",
                        destination                 = "CH",
                        master_version              = {143, otProductVersion},
                    }))
                else
                    --TODO: Have the OTT daemon respond/ask version info correctly. Set that version info here.
                    --  Something like:
                    --      prx.queueRequest(makeThermoRequest(4, openThermTimeout, {
                    --          method                      = "set_var",
                    --          destination                 = "DA",
                    --          version_info                = {159, otProductVersion},
                    --      }))
                end
            end

            if self.thermoExtension then
                local sleepFor, requestsQueued = StateManager.pollTrigger(interface, temp, Tif.SourceDest.CentralHeater)
                dbg(4, "Transparent slave parameters are ", (self.tspSupported and "supported") or "not supported")
                if self.tspSupported then
                    local sleepFor2, requestsQueued2 = StateManager.pollTrigger(interface, temp.tspTable, "SP", specialThermoRequest, tspVocabulary.vendors[self.module.vendorName] or genericTspTable)
                    sleepFor = math.min(sleepFor, sleepFor2)
                    requestsQueued = requestsQueued + requestsQueued2
                end
                if self.cooling then
                    sleepFor = math.min(30, sleepFor)
                    interface:sendMSS(6)
                elseif temp.variables.coolingModulationLevel ~= 0 then
                    proxies.thermo.queueRequest(makeThermoRequest(4, openThermTimeout, {
                        method                      = "WriteData",
                        destination                 = "CH",
                        cooling_modulation_level    = 0,
                    }))
                end
                scheduler:suspend(sleepFor)
            elseif self.openThermGateway then
                if self.openThermGateway.overrideMode == "off" then
                    dbg(3, "Not in overrideMode.")
                    scheduler:suspend(1)
                else
                    -- Make sure the master_slave_status is not drowned out by other communication.
                    if temp.forceMSS or now > (temp.nextMSS or 0) then
                        interface:sendMSS((temp.forceMSS and 2) or 6)
                        temp.nextMSS = now + 60
                    end

                    local sleepFor, requestsQueued = StateManager.pollTrigger(interface, temp, Tif.SourceDest.OpenThermSlave)
                    dbg(4, "Transparent slave parameters are ", (self.tspSupported and "supported") or "not supported")
                    if self.tspSupported then
                        local sleepFor2, requestsQueued2 = StateManager.pollTrigger(interface, temp.tspTable, "SP", specialRequest, tspVocabulary.vendors[self.module.vendorName] or genericTspTable)
                        sleepFor = math.min(sleepFor, sleepFor2)
                        requestsQueued = requestsQueued + requestsQueued2
                    end
                    if          self.intendedDHWSetpoint
                            and self.intendedDHWSetpoint ~= temp.variables.domesticHotWaterSetpoint
                            and     (temp.intendedDHWSetpointWritten or 0)
                                +  ((temp.unknownDataIds.domestic_hot_water_setpoint and 600) or 30) < now then
                        proxies.otgateway.queueRequest(makeThermoRequest(4, openThermTimeout, {
                            method                      = "WriteData",
                            destination                 = "OS",
                            domestic_hot_water_setpoint = self.intendedDHWSetpoint,
                        }))
                        requestsQueued = requestsQueued + 1
                    end

                    if          self.intendedMaximumBoilerTemperature
                            and self.intendedMaximumBoilerTemperature ~= temp.variables.maximumBoilerTemperature
                            and     (temp.intendedMaximumBoilerTemperatureWritten or 0)
                                +  ((temp.unknownDataIds.maximum_boiler_setpoint and 600) or 30) < now then
                        proxies.otgateway.queueRequest(makeThermoRequest(4, openThermTimeout, {
                            method                      = "WriteData",
                            destination                 = "OS",
                            maximum_boiler_setpoint     = self.intendedMaximumBoilerTemperature,
                        }))
                        requestsQueued = requestsQueued + 1
                    end

                    if          self.regulation
                            and self.regulation.enabled
                            and self.intendedControlSetpoint
                            and (   (   self.intendedControlSetpoint ~= temp.variables.controlSetpoint
                                    and (temp.intendedControlSetpointWritten or 0) + 10 < now)
                                or      (temp.intendedControlSetpointWritten or 0) + 90 < now) then
                        proxies.otgateway.queueRequest(makeThermoRequest(4, openThermTimeout, {
                            method                      = "WriteData",
                            destination                 = "OS",
                            control_setpoint            = self.intendedControlSetpoint,
                        }))
                        requestsQueued = requestsQueued + 1
                        temp.intendedControlSetpointWritten = now
                    end

                    for varName, tab in pairs(self.intended) do
                        local retry = (temp.unknownDataIds[varName] and 3600) or 300
                        if tab.value == temp.variables[varName] then
                            -- Value from the OM and OS are equal no need te resend
                            self.intended[varName] = nil
                        elseif now > tab.stamp + retry then
                            proxies.otgateway.queueRequest(makeThermoRequest(4, openThermTimeout, {
                                method              = "WriteData",
                                destination         = "OS",
                                [varName]           = tab.value,
                                max_tries           = 2,
                            }))
                            requestsQueued = requestsQueued + 1
                            self.intended[varName].stamp = now
                        end
                    end

                    for varName, settings in pairs(variableInfo) do
                        if          settings.masterLeading
                                and settings.retryIndefinitely
                                and temp.variables[varName] ~= nil
                                and (not equal(temp.variables[varName], self.actual[varName]))
                                and (temp.writeDataAttempted[varName] or 0) + 300 < now then
                            proxies.otgateway.queueRequest(makeThermoRequest(4, openThermTimeout, {
                                method              = "WriteData",
                                destination         = "OS",
                                [varName]           = temp.variables[varName],
                                max_tries           = 2,
                            }))
                            requestsQueued = requestsQueued + 1
                            temp.writeDataAttempted[varName] = now
                        end
                    end

                    -- Repeat setting remote_parameters every so often.
                    if temp.checkRemoteParametersAt then
                        if temp.checkRemoteParametersAt < now then
                            interface:setRemoteParameters()
                        else
                            sleepFor = math.min(sleepFor, temp.checkRemoteParametersAt - now)
                        end
                    end

                    scheduler:suspend(sleepFor)
                end
            else
                scheduler:suspend(3)
            end
        end
    end
    temp.pollThread = coroutine.create(pollWorker)
    scheduler:register(temp.pollThread)
    local function pollTrap(sched, thread, success, err)
        if not success then
            local msg = err.."\t"..debug.traceback(thread)
            dbg(1, "The pollThread crashed: ", msg)
            if self.id then
                temp.pollThread = coroutine.create(pollWorker)
                scheduler:register(temp.pollThread)
                scheduler.traps[temp.pollThread] = pollTrap
            end
        end
        scheduler.traps[thread] = nil
    end
    scheduler.traps[temp.pollThread] = pollTrap
end

local function destroyPollWorker(self)
    local temp = factory:getTemp(self)
    if temp.pollThread then
        scheduler:remove(temp.pollThread)
        scheduler.traps[temp.pollThread] = nil
        temp.pollThread = nil
    end
end

local function provideTSPMetaTable(self)
    local temp = factory:getTemp(self)
    return setmetatable({}, {
        __index = self.tsp,
        __newindex = function(t, k, v)
            temp.variables:set("slaveParameters", {k, v})
        end,
    })
end

local parentInit = class.init
function class.init(self, id)
    local temp = factory:getTemp(self)
    local ok, err = parentInit(self, id)
    if ok then
        self.openThermVersion       = "0.0"
        temp.tspTable               = {}
        temp.tspTable.modifiedPollterval = {}
        temp.tspTable.varUpdateRecorded  = {}
        temp.tspTable.varUpdated    = {}
        temp.writeDataAttempted     = {}
        temp.unknownDataIds         = {}
        temp.modifiedPollterval     = {}
        self.DataInvalidResponses   = {}
        self.intended               = {}
        self.actual                 = {}
        self.offset                 = {}
        self.intendedDHWState       = "on"
        self.intendedTsp            = {}
        self.tsp                    = {}
        self.tspSupported           = false
        self.smartgridControl       = "smart_grid_off"
        self.testMode               = "off"
        temp.tspTable.variables     = provideTSPMetaTable(self)
        for i=0, 255, 1 do
            self.tsp[i]             = false
            self.intendedTsp[i]     = false
        end
        function temp.isInitialized()
            return      (self.openThermGateway and true)
                    or  (package.loaded["rules.thermo.Initializer"] and package.loaded["rules.thermo.Initializer"].isInitialized())
        end
    end
    return ok, err
end
local parentPostCreate = class.postCreate
function class.postCreate(self)
    local temp = factory:getTemp(self)
    local interface = factory:getInterface(self)

    temp.boilerStatus.master    = boilerUtil.makeObservableMap(self, defineMasterStatusObservers(self))
    temp.boilerStatus.slave     = boilerUtil.makeObservableMap(self, defineSlaveStatusObservers(self))
    temp.variables              = boilerUtil.makeObservableMap(self, defineVariableObservers(self), function(a)
                                    return (type(a) == "string" and string.toCamelCase(a)) or a
                                end)
    if proxies.otgateway then
        proxies.otgateway.queueRequest(makeThermoRequest({
            destination             = Tif.SourceDest.Daemon,
            method                  = "publish_opentherm_cache",
        }))
        eventManager.setCallback({event = {name = "^proxy%.opened$"}}, function(source, event, time)
            if self.deletedDate == config.noDate then
                if event.proxy == "otgateway" then
                    dbg(3, "setting forceMSS to true")
                    temp.forceMSS = true
                    interface:sendMSS(2)
                    createPollWorker(self)
                end
            end
        end)
    end
    eventManager.setCallback({source = "OpenThermBoiler", event = {name = "^attached$"}}, function(source, event, time)
        local cName = event.other.class
        if          self.deletedDate    == config.noDate
                and source.id           == self.id
                and (   cName           == "ThermoExtension"
                    or  cName           == "OpenThermGateway") then
            createPollWorker(self)
        end
    end)
    return parentPostCreate(self)
end

function class.friendFunctions.onInitializationComplete(self)
    local temp = factory:getTemp(self)
    -- Initializer knows no CH. So reset temp.varUpdated and have StateManager.pollTrigger manage things for us.
    for k, v in pairs(temp.varUpdated) do
        temp.varUpdated[k] = nil
    end
    scheduler:resume(temp.pollThread)
end

local parentDelete = class.friendFunctions.delete
function class.friendFunctions.delete(self)
    destroyPollWorker(self)
    return parentDelete(self)
end
local parentUndelete = class.friendFunctions.unDelete
function class.friendFunctions.unDelete(self)
    local ok, err = parentUndelete(self)
    if not ok then return nil, err end
    createPollWorker(self)
    return true
end

local parentSetRemoteParameters = class.friendFunctions.setRemoteParameters
function class.friendFunctions.setRemoteParameters(self)
    local interface = factory:getInterface(self)
    interface:sendRemoteParameter('remote_parameter_flags',         remoteParameterFlags.encode({DHWReadable = self.DHWReadable, maxCHSetpointReadable = self.maxCHSetpointReadable}))
    return parentSetRemoteParameters(self)
end

local function remoteRequestHandler(self, index)
    local ott                   = self.openThermGateway.openThermThermostat
    local ottVendorName         = ott and ott.module.vendorName
    local ottVendorModel        = ott and ott.module.vendorModel
    local interface             = factory:getInterface(self)
    local strategy              = (otSetpointStrategies[ottVendorName] and otSetpointStrategies[ottVendorName][ottVendorModel]) or "default"
    dbgf(5, "The OpenTherm strategy is %s.", strategy)
    if strategy == "celcia20" and index == 170 then
        local varName = "room_setpoint"
        return interface:roomSetpointHandler(varName, 0)
    else
        return "UnknownDataId", 0
    end
end

function class.friendFunctions.responseHandler(self, request, response)
    local temp      = factory:getTemp(self)
    local interface = factory:getInterface(self)
    if not temp.isInitialized() then dbg(5,"not initialized") return 1 end
    local resp, req     = response.payload, request and request.payload
    local now           = scheduler.time()
    self.lastCommunicationDate   = now
    local idName        = request.variable or req.idName or resp.idName
    if idName and resp.messageType then
        if      resp.messageType == "UnknownDataId" then
            temp.unknownDataIds[idName] = now
        elseif  resp.messageType:match("Ack$") then
            temp.unknownDataIds[idName] = nil
        end
    end
    if      response.source == "CH" then
        if      resp.messageType == "ReadAck" then
            if not request.processed then
                StateManager.variableUpdate(temp, request.variable, resp.value)
            end
        elseif  resp.messageType == "WriteAck" then
            if not request.processed then
                if request.variable:match("^slave_parameters") then
                    StateManager.variableUpdate(temp, "slave_parameters", resp.value)
                elseif request.variable ~= "room_setpoint" then -- Ignore WriteAck(room_setpoint) from boilers connected to Anna.
                    StateManager.variableUpdate(temp, request.variable, request[request.variable])
                end
            end
        elseif      resp.messageType == "UnknownDataId"
                or (resp.messageType == "DataInvalid"   and req.messageType == "ReadData") then
            -- Don't retry unknown data ids too often.
            temp.varUpdated[request.variable]           = now
        end
    elseif response.source == "OS" and response.destination == "OM" then -- Adam listen mode
        if  not (req and resp) then
            sdbg(2,  "(Adam listen-mode) incomplete message:", req, resp)
        elseif  resp.messageType == "ReadAck" then
            StateManager.variableUpdate(temp, resp.idName,  resp.value)

            self.DataInvalidResponses[resp.idName]      = nil
        elseif  resp.messageType == "WriteAck" then
            if       req.idName == "master_slave_status" then
                interface:handleMasterSlaveStatus(req.value, "OM")
            elseif  req.idName == "room_setpoint" then
                interface:roomSetpointHandler("room_setpoint", req.value)
            else
                StateManager.variableUpdate(temp, req.idName,   req.value)
            end

            self.DataInvalidResponses[resp.idName]      = nil
        elseif  resp.messageType == "DataInvalid" or resp.messageType == "UnknownDataId" then
            -- Don't retry unknown data ids too often.
            temp.varUpdated[resp.idName]                = now

            if not (variableInfo[resp.idName] and variableInfo[resp.idName].alwaysAck) then
                self.DataInvalidResponses[resp.idName]  = true
            end
        else
            sdbg(2,  "(Adam listen-mode) unprocessed response:", request, response)
        end
    elseif response.source == "OS" and response.destination == "SM" then -- Adam override mode
        if req.messageType == "WriteData" then
            -- Received a WriteAck, meaning the writeData succeeded.
            if self.intended[resp.idName] and self.intended[resp.idName].value == req[resp.idName] then
                self.intended[resp.idName] = nil
            end
            if resp.messageType == 'ReadAck' then
                dbg(3, "Received a ReadAck on a WriteData, which is not compliant but we can interpret it correctly.")
                resp.messageType = 'WriteAck'
            end
            if          resp.messageType    == "DataInvalid"
                    or  resp.messageType    == "UnknownDataId" then

                if not (variableInfo[resp.idName] and variableInfo[resp.idName].alwaysAck) then
                    self.DataInvalidResponses[resp.idName] = true
                end
            elseif  resp.messageType        == "WriteAck" then
                self.DataInvalidResponses[req.idName]   = nil

                if          resp.idName == "control_setpoint"
                        and self.regulation
                        and self.regulation.enabled then
                    if not request.processed then
                        StateManager.variableUpdate(temp, req.idName, req.value)
                    end
                elseif      variableInfo[resp.idName]
                        and variableInfo[resp.idName].masterLeading then
                    self.actual[resp.idName] = req.value
                    temp.varUpdated[resp.idName] = now
                elseif  not request.processed then
                    StateManager.variableUpdate(temp, req.idName, req.value)
                else
                    temp.varUpdated[resp.idName] = now
                end
            end
        elseif req.messageType == "ReadData" then
            if          resp.messageType    == "DataInvalid"
                    or  resp.messageType    == "UnknownDataId" then
                -- Don't retry unknown data ids too often.
                if resp.idName == "slave_parameters" and resp.messageType == "DataInvalid" then
                    self.tsp[response.payload.value[1]] = false
                    temp.tspTable.modifiedPollterval = temp.tspTable.modifiedPollterval or {}
                    temp.tspTable.modifiedPollterval[response.payload.value[1]] = 18000 -- 5 hours
                    temp.tspTable.varUpdated[response.payload.value[1]] = now
                elseif resp.idName == "slave_parameters" and resp.messageType == "UnknownDataId" then
                    for i=0,255,1 do
                        self.tsp[i] = false
                    end
                    self.tspSupported = false
                else
                    temp.varUpdated[resp.idName]            = now
                    if not (variableInfo[resp.idName] and variableInfo[resp.idName].alwaysAck) then
                        self.DataInvalidResponses[resp.idName] = true
                    end
                end
            elseif  resp.messageType        == "ReadAck" then
                self.DataInvalidResponses[req.idName]   = nil
                if not request.processed then
                    StateManager.variableUpdate(temp, resp.idName, resp.value)
                end

                if resp.idName == "master_slave_status" then
                    temp.forceMSS = nil
                end
            end
        end
    elseif response.source == "DA" then
        -- Response from the OT talker daemon.

        if request.method == "set_remote_parameter_value" then
            temp.remoteParameters[request.idName] = request.value
        end
    end
end
function class.friendFunctions.errorHandler(self, request, response)
    local temp = factory:getTemp(self)
    if not temp.isInitialized() then dbg(5,"not initialized") return 1 end
    local now = scheduler.time()
    if not response.errorName:find("Timeout") then
        self.lastCommunicationDate = now
    end
    local req = request and request.payload
    -- Retry WriteData three times, at slightly lower priority.
    if request.payload and request.payload.messageType == "WriteData" then
        local tries = (request.tries or 0) + 1
        if tries < (request.max_tries or 5) then
            ((self.thermoExtension and proxies.thermo) or proxies.otgateway).queueRequest(makeThermoRequest(3, openThermTimeout, {
                method              = "WriteData",
                destination         = "OS",
                [req.idName]        = request.payload.value,
                tries               = tries,
            }))
        elseif variableInfo[req.idName] and variableInfo[req.idName].retryIndefinitely then
            self.intended[req.idName] = {
                value = request.payload.value,
                stamp = now,
            }
        end
    end
end

function class.friendFunctions.requestHandler(self, request)
    local temp = factory:getTemp(self)
    local interface = factory:getInterface(self)
    assert(request, "Need a request")
    assert(request.source == Tif.SourceDest.OpenThermMaster, "We only handle requests from the OpenThermMaster")

    local command       = request.payload.messageType
    local varName       = request.payload.idName
    local definition    = Tif.variables[dest][varName]
    local allowed       = definition and definition.access:match(command:sub(1,1))

    -- If the OpenThermMaster sends an DataInvalid request, we need to respond with DataInvalid if we recognize the ID, and UnknownDataId otherwise.
    -- If the OpenThermMaster tries to read or write an ID that is not read- or writable, return DataInvalid.
    if command == "DataInvalid" or not allowed then
        return {
            sequence        = request.sequence,
            destination     = request.source,
            variable        = varName,
            payload         = OTPayload.make((definition and "DataInvalid") or "UnknownDataId", varName, (definition.type == "2uint8" and {0, 0}) or 0),
        }
    end

    local responseType, value

    if      variableInfo[varName] and variableInfo[varName].alwaysUnknown then
        responseType    = "UnknownDataId"
        value           = (Tif.variables[dest][varName].type == "2uint8" and {0, 0}) or 0
    elseif  command == "WriteData" then
        if  varName == "slave_parameters" then
            value = request.payload.value
            if temp.variables.slaveParameterNumber[1] < value[1] and self.tspSupported then
                responseType = "DataInvalid"
            else
                if self.tsp[value[1]] == value[2] and self.tspSupported then
                    responseType    = "WriteAck"
                else
                    responseType    = tspGuard(self, temp.tspTable, value[1], value[2], command)
                end
            end
        elseif  varName == "remote_request" then
            responseType, value = remoteRequestHandler(self)
        elseif      varName == "domestic_hot_water_setpoint"
                or  varName == "maximum_boiler_setpoint"        then
            -- The OTThermostat may not modify this data, as we've indicated; so tell it not to.
            responseType = "DataInvalid"
        elseif      varName:match('ch2') and temp.variables.slaveConfiguration and not self.withSecondaryHeatingCircuit then
            -- The OTBoiler has no second heating circuit, and thus no knowledge of any relevant IDs.
            responseType = "UnknownDataId"
        elseif  varName == "room_setpoint" then
            value = request.payload.value
            interface:roomSetpointHandler("room_setpoint", value)
        elseif      "control_setpoint" == varName
                and self.regulation
                and self.regulation.enabled then
            -- Don't write through, we know better.
            responseType = "WriteAck"
        elseif      variableInfo[varName] then
            if variableInfo[varName].masterLeading then
                responseType = "WriteAck"
                temp.variables:set(string.toCamelCase(varName), request.payload.value)

                -- Write through if stale or if changed.
                if          os.time() - (temp.varUpdated[varName] or 0) > 300
                        or  not equal(self.actual[varName], request.payload.value) then
                    proxies.otgateway.queueRequest(makeThermoRequest(2, openThermTimeout, {
                        method              = "WriteData",
                        destination         = "OS",
                        [varName]           = request.payload.value,
                        max_tries           = 2,
                    }))
                end
            elseif variableInfo[varName].acceptWithoutPassthrough then
                responseType = "WriteAck"
                temp.variables:set(string.toCamelCase(varName), request.payload.value)
            end
        end

        if not responseType then
            -- Write through the ID, even if the OTSlave did nothing with it; other values may return other responses.
            responseType = (self.DataInvalidResponses[varName] and "DataInvalid") or "WriteAck"
            -- temp.variables:set(string.toCamelCase(varName), request.payload.value)

            -- check if we need to retry indefinitely
            if variableInfo[varName] and variableInfo[varName].retryIndefinitely then
                self.intended[varName] = {  value   = request.payload.value,
                                            stamp   = scheduler.time() }
            end

            proxies.otgateway.queueRequest(makeThermoRequest(2, openThermTimeout, {
                method              = "WriteData",
                destination         = "OS",
                [varName]           = request.payload.value,
                max_tries           = 2,
            }))
        end

        if responseType == "WriteAck" and not value then
            value = request.payload.value
        end
    elseif      varName == "remote_override_room_setpoint"
            or  varName == "remote_override_function" then

        value = interface:roomSetpointHandler(varName)
    elseif  varName == "remote_parameter_flags" then
        -- Do not allow the OpenThermThermostat to set the domestic_hot_water_temperature and the maximum_boiler_temperature; we are setting those ourselves.
        value = remoteParameterFlags.encode({DHWReadable = self.DHWReadable, maxCHSetpointReadable = self.maxCHSetpointReadable})
    elseif  varName == "slave_parameters" then
        if request.payload.value and request.payload.value[1] and self.tsp[request.payload.value[1]] then
            responseType = "ReadAck"
            value = {request.payload.value[1], self.tsp[request.payload.value[1]]}
        else
            responseType = "DataInvalid"
            value = {request.payload.value[1] or 0,0}
        end
    end

    value               = value or temp.variables[string.toCamelCase(varName)]
    responseType        = responseType or ((varName and ((value and "ReadAck") or "DataInvalid")) or "UnknownDataId")

    return {
        sequence        = request.sequence,
        destination     = request.source,
        variable        = varName,
        payload         = OTPayload.make(responseType, varName, value),
    }
end

function class.friendFunctions.setOpenThermConfigOwner(self, paramArray, enabled)
    if proxies.otgateway then
        local anon_array = {}
        for _, param in ipairs(paramArray) do
            local tab      = {}
            local id = tonumber(Tif.variables.OS[param].id)
            table.insert(tab, id)
            table.insert(tab, (enabled and "CR") or "OM")
            table.insert(tab, "WO")
            table.insert(tab, "Active")
            table.insert(tab, daemonConfig.parameters[param].priority)
            table.insert(tab, daemonConfig.parameters[param].event_handling)
            table.insert(tab, daemonConfig.parameters[param].minimum_interval)
            table.insert(anon_array, tab)
        end
        proxies.otgateway.queueRequest(makeThermoRequest(2, 3, {
            destination = Tif.SourceDest.Daemon,
            method      = "set_opentherm_config",
            anonymous_array = anon_array
        }))
    end
end

local parentSwitchRegulationState = class.friendFunctions.switchRegulationState
function class.friendFunctions.switchRegulationState(self, newState)
    local temp = factory:getTemp(self)
    if self.regulation then
        local previouslyEnabled = self.regulation.enabled
        parentSwitchRegulationState(self, newState)
        if self.regulation.enabled and not previouslyEnabled then
            self.intendedControlSetpoint = 0
            temp.forceMSS = true
            factory:getInterface(self):sendMSS(2)                       --FIXME: Useless message for an OnOffBoiler
        end
    end
end

-- Variable updates go into temp.variables or temp.boilerStatus and are handled there by the configurable logging code.
function class.friendFunctions.setBoilerStatus(                 self, time, newBoilerStatus)
    factory:getInterface(self):handleMasterSlaveStatus(newBoilerStatus, "SE")
end
function class.friendFunctions.setOtOemFaultCode(               self, time, code)
    local temp = factory:getTemp(self)
    temp.variables:set("otFaultCodes", {(temp.variables.otFaultCodes and temp.variables.otFaultCodes[1]) or 0, code})
end
function class.friendFunctions.setOtApplicationSpecificFault(   self, time, value)
    local temp = factory:getTemp(self)
    temp.variables:set("otFaultCodes", {value, (temp.variables.otFaultCodes and temp.variables.otFaultCodes[2]) or 0})
end
function class.friendFunctions.setModulationLevel(              self, time, modulationLevel)
    factory:getTemp(self).variables:set("relativeModulationLevel", modulationLevel)
end
function class.friendFunctions.setBoilerTemperature(            self, time, temperature)
    factory:getTemp(self).variables:set("boilerWaterTemperature", temperature)
end
function class.friendFunctions.setIntendedBoilerTemperature(    self, time, temperature)
    if          (not self.regulation)
            or  (not self.regulation.enabled) then
        factory:getTemp(self).variables:set("controlSetpoint", temperature)
    end
end
function class.friendFunctions.setReturnWaterTemperature(       self, time, temperature)
    factory:getTemp(self).variables:set("returnWaterTemperature", temperature)
end
function class.friendFunctions.setMaximumBoilerTemperatureBounds(self, bounds)
    factory:getTemp(self).variables:set("boilerSetpointBounds", bounds)
end
function class.friendFunctions.setMaximumBoilerTemperature(     self, time, temperature)
    factory:getTemp(self).variables:set("maximumBoilerSetpoint", temperature)
end
function class.friendFunctions.setMaxCHSetpointAccess(          self, access)
    if      access:match("[Ww]") then
        self.maxCHSetpointReadable = true
        self.maxCHSetpointWritable = true
    elseif  access:match("[Rr]") then
        self.maxCHSetpointReadable = true
        self.maxCHSetpointWritable = false
    end
    processRemoteParameterFlags(self)
end

function class.friendFunctions.setDHWTemperature(               self, time, temperature)
    if not self.DHWReadable then
        self.DHWReadable = true
        processRemoteParameterFlags(self)
    end
    factory:getTemp(self).variables:set("domesticHotWaterTemperature", temperature)
end
function class.friendFunctions.setDHWBounds(                    self, bounds)
    factory:getTemp(self).variables:set("domesticHotWaterBounds", bounds)
end
function class.friendFunctions.setDHWSetpoint(                  self, time, temperature)
    factory:getTemp(self).variables:set("domesticHotWaterSetpoint", temperature)
end
function class.friendFunctions.setDHWSetpointAccess(            self, access)
    if      access:match("[Ww]") then
        self.DHWReadable = true
        self.DHWWritable = true
    elseif  access:match("[Rr]") then
        self.DHWReadable = true
        self.DHWWritable = false
    end
    processRemoteParameterFlags(self)
end
function class.friendFunctions.setControlSetpoint(              self, setpoint)
    local temp = factory:getTemp(self)
    dbgf(3, "Received new control_setpoint: %.2f -> %.2f", temp.variables.controlSetpoint or 0/0, setpoint)
    if self.module.vendorName == "Techneco" and setpoint >= 44 and setpoint < 52 then
        setpoint = 52
        dbgf(3, "As this is a Techneco Elga, adjust control_setpoint to %.2f to make it clear we wish Elga to engage the boiler.", setpoint)
    end
    self.intendedControlSetpoint = setpoint
    if          self.regulation
            and self.regulation.enabled
            and self.openThermGateway and self.openThermGateway.overrideMode == "on" --FIXME: Expected openThermGateway, but not created yet
            and self.intendedControlSetpoint ~= temp.variables.controlSetpoint  then
        local chOn, oldCHOn = setpoint == 0, (temp.variables.controlSetpoint or 0) == 0
        if chOn ~= oldCHOn then
            factory:getInterface(self):handleMasterSlaveStatus(temp.variables.masterSlaveStatus or {2, 0}, "OS")
        end
        proxies.otgateway.queueRequest(makeThermoRequest(3, openThermTimeout, {
            method                      = "WriteData",
            destination                 = "OS",
            control_setpoint            = self.intendedControlSetpoint,
        }))
    end
end

local function tspCheck(self, setpoint, service)
    -- Temporary behaviour, to be replaced by proper checks.
    local vName = self.module.vendorName
    local definition = tspVocabulary.vendors[vName]
    if definition then
        for key, val in pairs(definition.variables) do
            if key == service.logType then
                return tonumber(val.id)
            end
        end
    end
end

function class.friendFunctions.setTemperatureOffset(self, offset, service)
    sdbg(5, "setTemperatureOffset:", offset, service)
    local varName = tspCheck(self, offset, service)
    if varName then
        tspGuard(self, factory:getTemp(self).tspTable, varName, offset, "WriteData")
    end
end

function class.friendFunctions.setTime(self, time, newVal, service)
    sdbg(5, "setTime:", time, newVal, service)
    local varName = tspCheck(self, newVal, service)
    if varName then
        tspGuard(self, factory:getTemp(self).tspTable, varName, newVal, "WriteData")
    end
end

function class.friendFunctions.setThreshold(self, time, newVal, service)
    sdbg(5, "setThreshold:", time, newVal, service)
    if self.thermoExtension and service.logType == "cooling_deactivation_threshold" then
        self.coolingDeactivationThreshold = newVal
        evaluateCooling(self, factory:getTemp(self))
        updateService(service, nil, time, newVal, service.resolution)
    else
        local varName = tspCheck(self, newVal, service)
        if varName then
            tspGuard(self, factory:getTemp(self).tspTable, varName, newVal, "WriteData")
        end
    end
end

-- Hardware specific code for Thermostat to call.
-- TODO: Block this call in monitor_mode!
function class.friendFunctions.setSetpoint(self, time, setpoint, service)
    local temp = factory:getTemp(self)
    local interface = factory:getInterface(self)

    sdbg(5, "setSetpoint:", time, setpoint, service)
    local definition    = tspVocabulary.vendors[self.module.vendorName]
    if definition and definition.logging[service.logType] then
        local transform     = definition.logging[service.logType].transform
        setpoint            =       (transform and "number" == type(setpoint) and ((transform.offset or 0) + setpoint * (transform.factor or 1)))
                                or  setpoint
    end
    local ext = self.thermoExtension
    if ext then
        if      service.logType == "domestic_hot_water_setpoint" then
            ext.domesticHotWaterSetpoint = setpoint
        elseif  service.logType == "maximum_boiler_temperature" then
            ext.maximumBoilerTemperature = setpoint
        elseif  service.logType == "cooling_activation_outdoor_temperature" then
            self.coolingActivationOutdoorTemperature = setpoint
            evaluateCooling(self, temp)
            service:newConsecutiveMeasurement(nil, scheduler.time(), setpoint)
        else
            local varName = tspCheck(self, setpoint, service)
            if varName then
                tspGuard(self, temp.tspTable, varName, setpoint, "WriteData")
            else
                return error("Unknown service "..tostring(service))
            end
        end
    elseif self.openThermGateway then
        if      service.logType == "domestic_hot_water_setpoint" then
            self.intendedDHWSetpoint        = setpoint
            temp.intendedDHWSetpointWritten = scheduler.time()

            proxies.otgateway.queueRequest(makeThermoRequest(3, openThermTimeout, {
                method                      = "WriteData",
                destination                 = "OS",
                domestic_hot_water_setpoint = setpoint,
                max_tries                   = 3
            }))
        elseif  service.logType == "maximum_boiler_temperature" then
            -- TODO: If regulation is active we are leading; else the boiler (as it is currently).
            -- Use e.g. temp.thermostat.maximum_boiler_temperature:newConsecutiveMeasurement(nil, scheduler.time(), setpoint)
            self.intendedMaximumBoilerTemperature           = setpoint
            temp.intendedMaximumBoilerTemperatureWritten    = scheduler.time()

            proxies.otgateway.queueRequest(makeThermoRequest(3, openThermTimeout, {
                method                      = "WriteData",
                destination                 = "OS",
                maximum_boiler_setpoint     = setpoint,
                max_tries                   = 3
            }))
        elseif  service.logType == "thermostat" then
            self.intendedSetpoint = setpoint
            interface:roomSetpointHandler("intended_setpoint")
        else
            local varName = tspCheck(self, setpoint, service)
            if varName then
                tspGuard(self, temp.tspTable, varName, setpoint, "WriteData")
            else
                return error("Unknown service "..tostring(service))
            end
        end
    end
end

function class.friendFunctions.switchDHWState(                  self, newState, isIntended)
    local temp = factory:getTemp(self)
    dbgf(5, "OpenThermBoiler:switchDHWState('%s', %s)", newState, tostring(isIntended))
    if not isIntended then
        passOnMeasurement(self.module, temp, nil, "DomesticHotWaterToggle", "domestic_hot_water_comfort_mode", nil, nil, newState, "")
    end

    self.intendedDHWState = newState
    if      self.thermoExtension then
        return self.thermoExtension:switchDHWState(newState)
    elseif  self.openThermGateway then
        local dhwEnabled = (self.intendedDHWState == "on")
        if dhwEnabled ~= temp.boilerStatus.master.DHWEnabled then
            StateManager.variableUpdate(temp, 'master_slave_status', {boilerStatus.split(boilerStatus.setDHWEnabled(boilerStatus.combine(unpack(temp.variables.masterSlaveStatus or {0, 0})), dhwEnabled))})
            temp.boilerStatus.master.DHWEnabled = dhwEnabled
            temp.forceMSS = true
            factory:getInterface(self):sendMSS(2)
        end
    end
end

function class.friendFunctions.evaluateCooling(self)
    evaluateCooling(self, factory:getTemp(self))
end

local parentBackup = class.friendFunctions.backup
function class.friendFunctions.backup(self,  backupwriter, filename)
    -- serialize the variables collected from the CH
    if self.openThermGateway then
        self.variables = factory:getTemp(self).variables:clone()
    end
    return parentBackup(self,  backupwriter, filename)
end

local dualTemperatureServices = {"maximum_boiler_temperature", "domestic_hot_water_setpoint"}

local parentRestore = class.friendFunctions.restore
function class.friendFunctions.restore(self, ...)
    local temp      = factory:getTemp(self)
    local interface = factory:getInterface(self)
    local ok, err   = parentRestore(self, ...)
    if not ok then return nil, err end

    -- Migrates Smile OT 1.0.x to Smile OT 1.0.2
    if nil == self.regulation and self.openThermRegulation then
        self.regulation = self.openThermRegulation
    end
    if self.deletedDate == config.noDate and self.regulation and self.regulation.enabled then
        interface:setOpenThermConfigOwner({"control_setpoint", "maximum_relative_modulation_level"}, self.regulation.enabled)
    end

    self.DataInvalidResponses = self.DataInvalidResponses or {}
    for varName, settings in pairs(variableInfo) do
        if settings.alwaysAck then
            self.DataInvalidResponses[varName] = nil
        end
    end

    if self.variables then
        for varName, value in pairs(self.variables) do
            if isAcceptableOTVariable(string.toSnakeCase(varName), value) then
                temp.variables:setUnderhand(varName, value)
            end
        end
        self.variables = nil
    end

    self.intendedDHWState               = self.intendedDHWState or "on"
    temp.tspTable                       = {}
    temp.tspTable.varUpdated            = {}
    temp.tspTable.variables             = provideTSPMetaTable(self)
    temp.tspTable.varUpdateRecorded     = {}
    temp.remoteParameters               = {}

    if self.module then
        local stateSerf     = self.module:iterateServices("BoilerState", "central_heating_state")()
        local stateFunc     = stateSerf and stateSerf:iterateFunctionalities()()
        local directObject  = stateFunc and stateFunc.directObject

        -- Migrate Smile OpenTherm 2.2.19 -> 2.2.20. TODO: Remove once obsolete.
        for i, serfType in ipairs(dualTemperatureServices) do
            local thermostat   = self.module:iterateServices("Thermostat",     "maximum_boiler_temperature")()
            local thermoMeter  = self.module:iterateServices("ThermoMeter",    "maximum_boiler_temperature")()

            if thermostat and thermoMeter then
                dbgf(3, "The %s Service exists as both a Thermostat and a ThermoMeter; deleting the ThermoMeter, keeping the Thermostat.", serfType)
                for func in thermoMeter:iterateFunctionalities() do
                    assert(func:detachService(thermoMeter))
                    assert(func:attachService(thermostat))
                end
                assert(self.module:detachService(thermoMeter))
                assert(thermoMeter:hardDelete())
                if thermostat:countFunctionalities() < 2 then
                    assert(directObject:attachService(thermostat))
                end
            end
        end

        -- Migrate Smile Thermo 1.6.x to 1.7.x. TODO: Remove once there are no 1.6.x-s left.
        provideService(self.module, directObject, "DomesticHotWaterToggle",     "domestic_hot_water_comfort_mode")

        -- Poll outside_temperature more often for Elga.
        if self.module.vendorName == "Techneco" then
            temp.modifiedPollterval.outside_temperature = 30

            if domObject.isAn("AMERegulation", self.regulation) and not tonumber(self.correctedElgaActivationThreshold) then
                self.regulation.openThermBoilerActivationThreshold  = 0
                self.regulation.gradualAdHocHeatingRate             = 99
                self.correctedElgaActivationThreshold               = os.time
            end
        end
    end

    if self.deletedDate ~= config.noDate then
        destroyPollWorker(self)
    else
        createPollWorker(self)
        if proxies.otgateway then
            interface:setRemoteParameters()
            proxies.otgateway.queueRequest(makeThermoRequest({
                destination             = Tif.SourceDest.Daemon,
                method                  = "publish_opentherm_cache",
            }))
            interface:sendMSS(2)
        end
    end
    return true
end

local res                       = {}
local sFormat, tInsert, tConcat = string.format, table.insert, table.concat
-- \friendmethod{toXML}{Render this OpenThermGateway to XML. Serializations are not cached.}{
-- \result{\myref{d:string}}{The serialized resource.}
-- }
function class.friendFunctions.toXML(self)
    local ext = self.openThermGateway or self.thermoExtension
    if ext then
        tInsert(res, sFormat('\t\t\t<open_therm_boiler id=\'%s\'>\n\t\t\t\t%s\n',
                        self.id, tagCache[ext]))
    else
        tInsert(res, sFormat('\t\t\t<open_therm_boiler id=\'%s\'>\n',
                        self.id))
    end
    if nil ~= self.openThermVersion then
        tInsert(res, sFormat('\t\t\t\t<open_therm_version>%s</open_therm_version>\n',
                                    self.openThermVersion))
    end
    if nil ~= self.isModulating then
        tInsert(res, sFormat('\t\t\t\t<is_modulating>%s</is_modulating>\n',
                                    self.isModulating))
    end
    if nil ~= self.maximumCapacity then
        tInsert(res, sFormat('\t\t\t\t<maximum_capacity>%s</maximum_capacity>\n',
                                    self.maximumCapacity))
    end
    if nil ~= self.minimumModulationLevel then
        tInsert(res, sFormat('\t\t\t\t<minimum_modulation_level>%s</minimum_modulation_level>\n',
                                    self.minimumModulationLevel))
    end
    if nil ~= self.withCooling then
        tInsert(res, sFormat('\t\t\t\t<with_cooling>%s</with_cooling>\n',
                                    self.withCooling))
    end
    if nil ~= self.withDomesticHotWater then
        tInsert(res, sFormat('\t\t\t\t<with_domestic_hot_water>%s</with_domestic_hot_water>\n',
                                    self.withDomesticHotWater))
    end
    if nil ~= self.withDomesticHotWaterStorage then
        tInsert(res, sFormat('\t\t\t\t<with_domestic_hot_water_storage>%s</with_domestic_hot_water_storage>\n',
                                    self.withDomesticHotWaterStorage))
    end
    if nil ~= self.withMasterLowOffAndPumpControl then
        tInsert(res, sFormat('\t\t\t\t<with_master_low_off_and_pump_control>%s</with_master_low_off_and_pump_control>\n',
                                    self.withMasterLowOffAndPumpControl))
    end
    if nil ~= self.withSecondaryHeatingCircuit then
        tInsert(res, sFormat('\t\t\t\t<with_secondary_heating_circuit>%s</with_secondary_heating_circuit>\n',
                                    self.withSecondaryHeatingCircuit))
    end
    if (tonumber(self.openThermVersion) or 0) > 2.2 then
        if nil ~= self.withRemoteWaterFillingFunction then
            tInsert(res, sFormat('\t\t\t\t<with_remote_water_filling_function>%s</with_remote_water_filling_function>\n',
                                    self.withRemoteWaterFillingFunction))
        end
        if nil ~= self.withMasterHeatCoolModeSwitching then
            tInsert(res, sFormat('\t\t\t\t<with_master_heat_cool_mode_switching>%s</with_master_heat_cool_mode_switching>\n',
                                    self.withRemoteWaterFillingFunction))
        end
    end
    -- Last two parameters are specific to Elga.
    if nil ~= self.smartgridControl then
        tInsert(res, sFormat('\t\t\t\t<smartgrid_control>%s</smartgrid_control>\n',
                                    self.smartgridControl))
    end
    if nil ~= self.testMode then
        tInsert(res, sFormat('\t\t\t\t<test_mode>%s</test_mode>\n',
                                    self.testMode))
    end
    tInsert(res, '\t\t\t</open_therm_boiler>\n')
    local resp = tConcat(res)
    for i = #res, 1, -1 do res[i] = nil end
    return resp
end

local function getAttributeConditionalOnOTProtocolVersion(self, attribute)
    -- Attribute was not defined for OT version 2.2 and lower; only return if OT version is higher.
    if (tonumber(self.openThermVersion) or 0) > 2.2 then
        return self[attribute.name]
    end
end

class
    -- \attribute{openThermVersion}{Stores the openThermVersion of the OpenThermBoiler, 0 by default.}
    :defineAttribute({  name    = "testMode",                       type = "string",  public = "set", friend = "set",
                        set     = function(self, attribute, val)
                            dbg(3, "testMode ", val)
                            assert(type(val) == "string" and testModeEnum[val], "Mode must be a either off, circulation, circulation_heat, circulation_heat_boiler, circulation_cooling")
                            if self.testMode ~= val then
                                self.testMode = val
                                tspGuard(self, factory:getTemp(self).tspTable, 84, testModeEnum[val], "WriteData")
                                return true
                            end
                            return false
                        end
                            })
    :defineAttribute({  name    = "openThermVersion",               type = "string",  public = "get", friend = "get"})
    :defineAttribute({  name    = "smartgridControl",               type = "string",  public = "set", friend = "set",
                        set     = function(self, attribute, val)
                            assert(smartgridControlEnum[val], "SmartgridControl must be a either smart_grid_off, default, heating_cooling_only state.")
                            if self.smartgridControl ~= smartgridControlEnum[val] then
                                self.smartgridControl = smartgridControlEnum[val]
                                tspGuard(self, factory:getTemp(self).tspTable, 88, val, "WriteData")
                                return true
                            end
                            return false
                        end
                            })
    :defineAttribute({  name    = "intendedDHWState",               type = "string",  public = "get", friend = "get",})
    :defineAttribute({  name    = "withDomesticHotWater",           type = "boolean", public = "get", friend = "get"})
    :defineAttribute({  name    = "isModulating",                   type = "boolean", public = "get", friend = "get"})
    :defineAttribute({  name    = "withCooling",                    type = "boolean", public = "get", friend = "get"})
    :defineAttribute({  name    = "withDomesticHotWaterStorage",    type = "boolean", public = "get", friend = "get",
                        get     = function(self, attribute)
                            -- This attribute is either set or unspecified, so return true or nil, never false.
                            return self[attribute.name] or nil
                        end,})
    :defineAttribute({  name    = "withMasterLowOffAndPumpControl", type = "boolean", public = "get", friend = "get"})
    :defineAttribute({  name    = "withSecondaryHeatingCircuit",    type = "boolean", public = "get", friend = "get"})
    :defineAttribute({  name    = "withRemoteWaterFillingFunction", type = "boolean", public = "get", friend = "get",
                        get     = getAttributeConditionalOnOTProtocolVersion,})
    :defineAttribute({  name    = "withMasterHeatCoolModeSwitching",type = "boolean", public = "get", friend = "get",
                        get     = getAttributeConditionalOnOTProtocolVersion,})
    :defineAttribute({  name    = "maximumCapacity",                type = "number",  public = "get", friend = "get"})
    :defineAttribute({  name    = "minimumModulationLevel",         type = "number",  public = "get", friend = "get"})

package.loaded['DomainObject.Protocol.Boiler.OpenThermBoiler'] = factory

return package.loaded['DomainObject.Protocol.Boiler.OpenThermBoiler']
