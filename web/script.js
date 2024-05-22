/*
This script file contains the main logic for the Grosbeak web application.
 
It defines functions for reading and validating user input, 
uploading files to the server, and handling button clicks and file changes.
*/

window.Ajv = window.ajv2020

// Read a file as text
function readAsText(inputElement) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader()
        reader.onload = (e) => {
            resolve(e.target.result)
        }
        reader.onerror = (e) => {
            reject(e)
        }
        reader.readAsText(inputElement.files[0])
    })
}

async function main() {
    const ajv = new Ajv()
    const matchScheduleSchema = await (await fetch("/schemas/match-schedule.json")).json()
    const validateMatchSchedule = ajv.compile(matchScheduleSchema)
    const teamListSchema = await (await fetch("/schemas/team-list.json")).json()
    const validateTeamList = ajv.compile(teamListSchema)

    const apiKeyInput = document.getElementById("apikey")
    const eventKeyInput = document.getElementById("eventkey")

    // Get event key and api key from input elements
    function getParams() {
        const apiKey = apiKeyInput.value
        const eventKey = eventKeyInput.value
        if (!eventKey) {
            alert("No event key")
            return null
        }
        if (!apiKey) {
            alert("No api key")
            return null
        }
        return {apiKey, eventKey}
    }

    // Send event key and api key to server on submit  
    async function uploadFile(type) {
        const params = getParams()
        if (params === null) {
            return
        }
        const {eventKey, apiKey} = params
        let input
        switch (type) {
            case "match-schedule":
                input = matchScheduleFileInput
                break;
            case "team-list":
                input = teamListFileInput
                break;
            default:
                return
        }
        const fileText = await readAsText(input)
        const data = JSON.parse(fileText)
        // Send event key, api key, and file data to server
        const response = await fetch("/admin/static", {
            method: "PUT", headers: {
                "Content-Type": "application/json", "Authorization": apiKey
            }, body: JSON.stringify({type, event_key: eventKey, data})
        })
        if (response.ok) {
            alert(`${type} uploaded successfully`)
        } else {
            alert(`Error uploading ${type}: ${await response.text()}`)
        }
    }

    const matchScheduleSubmitButton = document.getElementById("submit-matchschedule")
    const matchScheduleFileInput = document.getElementById("matchschedule")
    // Whenever the match schedule file input is changed, verify the file is valid and update the button accordingly
    matchScheduleFileInput.addEventListener("change", async (v) => {
        const fileText = await readAsText(v.target)
        const valid = validateMatchSchedule(JSON.parse(fileText))
        if (valid) {
            matchScheduleSubmitButton.disabled = false
        } else {
            matchScheduleSubmitButton.disabled = true
            alert("Invalid match schedule")
        }
    })
    // Upload match schedule on match schedule submit
    matchScheduleSubmitButton.addEventListener("click", async () => {
        uploadFile("match-schedule")
    })
    const teamListSubmitButton = document.getElementById("submit-teamlist")
    const teamListFileInput = document.getElementById("teamlist")
    // Whenever the team list file input is changed, verify the file is valid and update the button accordingly
    teamListFileInput.addEventListener("change", async (v) => {
        const fileText = await readAsText(v.target)
        const valid = validateTeamList(JSON.parse(fileText))
        if (valid) {
            teamListSubmitButton.disabled = false
        } else {
            teamListSubmitButton.disabled = true
            alert("Invalid team list")
        }
    })
    // Upload team list on team list submit
    teamListSubmitButton.addEventListener("click", async () => {
        uploadFile("team-list")
    })
    const picklistSheetIdInput = document.getElementById("picklist")
    const picklistSheetSubmitButton = document.getElementById("submit-picklist")
    // Whenever the sheet id input is changed, verify the sheet id is not blank and update the button accordingly
    picklistSheetIdInput.addEventListener("input", (ele, ev) => {
        picklistSheetSubmitButton.disabled = ele.data === null
    })
    // Send event key and sheet id on picklist submit
    picklistSheetSubmitButton.addEventListener("click", async () => {
        const sheetId = picklistSheetIdInput.value
        const params = getParams()
        if (params === null) {
            return
        }
        const {eventKey, apiKey} = params
        // Send event key and sheet id to server
        const response = fetch("/admin/sheet-id", {method: "POST", headers: {
            "Content-Type": "application/json", "Authorization": apiKey
        }, body: JSON.stringify({event_key: eventKey, sheet_id: sheetId})})
        if (response.ok) {
            alert(`Set sheet id for ${eventKey} successfully`)
        } else {
            alert(`Error setting sheet id: ${await response.text()}`)
        }
    })
}

main()
