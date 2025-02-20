URL: https://gpo.congress.gov/
---
[skip to main content](https://gpo.congress.gov/#content)

# Congress.gov API

## Congress.gov API

```
[ Base URL: /v3 ]
```

Congress.gov shares its application programming interface (API) with the public to ingest the Congressional data. [Sign up for an API key](https://gpo.congress.gov/sign-up/) from api.data.gov that you can use to access web services provided by Congress.gov. To learn more, view our [GitHub repository](https://github.com/LibraryOfCongress/api.congress.gov/).

Authorize

#### [bill](https://gpo.congress.gov/\#/bill)     Returns bill data from the API

GET[/bill](https://gpo.congress.gov/#/bill/bill_list_all)

Returns a list of bills sorted by date of latest action.

GET[/bill/{congress}](https://gpo.congress.gov/#/bill/bill_list_by_congress)

Returns a list of bills filtered by the specified congress, sorted by date of latest action.

GET[/bill/{congress}/{billType}](https://gpo.congress.gov/#/bill/bill_list_by_type)

Returns a list of bills filtered by the specified congress and bill type, sorted by date of latest action.

GET[/bill/{congress}/{billType}/{billNumber}](https://gpo.congress.gov/#/bill/bill_details)

Returns detailed information for a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/actions](https://gpo.congress.gov/#/bill/bill_actions)

Returns the list of actions on a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/amendments](https://gpo.congress.gov/#/bill/bill_amendments)

Returns the list of amendments to a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/committees](https://gpo.congress.gov/#/bill/bill_committees)

Returns the list of committees associated with a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/cosponsors](https://gpo.congress.gov/#/bill/bill_cosponsors)

Returns the list of cosponsors on a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/relatedbills](https://gpo.congress.gov/#/bill/bill_relatedbills)

Returns the list of related bills to a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/subjects](https://gpo.congress.gov/#/bill/bill_subjects)

Returns the list of legislative subjects on a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/summaries](https://gpo.congress.gov/#/bill/bill_summaries)

Returns the list of summaries for a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/text](https://gpo.congress.gov/#/bill/bill_text)

Returns the list of text versions for a specified bill.

GET[/bill/{congress}/{billType}/{billNumber}/titles](https://gpo.congress.gov/#/bill/bill_titles)

Returns the list of titles for a specified bill.

GET[/law/{congress}](https://gpo.congress.gov/#/bill/law_list_by_congress)

Returns a list of laws filtered by the specified congress.

GET[/law/{congress}/{lawType}](https://gpo.congress.gov/#/bill/law_list_by_congress_and_lawType)

Returns a list of laws filtered by specified congress and law type (public or private).

GET[/law/{congress}/{lawType}/{lawNumber}](https://gpo.congress.gov/#/bill/law_list_by_congress_lawType_and_lawNumber)

Returns a law filtered by specified congress, law type (public or private), and law number.

#### [amendments](https://gpo.congress.gov/\#/amendments)     Returns amendment data from the API

GET[/amendment](https://gpo.congress.gov/#/amendments/Amendment)

Returns a list of amendments sorted by date of latest action.

GET[/amendment/{congress}](https://gpo.congress.gov/#/amendments/Amendmentcongress)

Returns a list of amendments filtered by the specified congress, sorted by date of latest action.

GET[/amendment/{congress}/{amendmentType}](https://gpo.congress.gov/#/amendments/Amendmentlist)

Returns a list of amendments filtered by the specified congress and amendment type, sorted by date of latest action.

GET[/amendment/{congress}/{amendmentType}/{amendmentNumber}](https://gpo.congress.gov/#/amendments/Amendmentdetails)

Returns detailed information for a specified amendment.

GET[/amendment/{congress}/{amendmentType}/{amendmentNumber}/actions](https://gpo.congress.gov/#/amendments/Amendmentactions)

Returns the list of actions on a specified amendment.

GET[/amendment/{congress}/{amendmentType}/{amendmentNumber}/cosponsors](https://gpo.congress.gov/#/amendments/Amendmentcosponsors)

Returns the list of cosponsors on a specified amendment.

GET[/amendment/{congress}/{amendmentType}/{amendmentNumber}/amendments](https://gpo.congress.gov/#/amendments/Amendmentamendments)

Returns the list of amendments to a specified amendment.

GET[/amendment/{congress}/{amendmentType}/{amendmentNumber}/text](https://gpo.congress.gov/#/amendments/amendmentsText)

Returns the list of text versions for a specified amendment from the 117th Congress onwards.

#### [summaries](https://gpo.congress.gov/\#/summaries)     Returns summaries data from the API

GET[/summaries](https://gpo.congress.gov/#/summaries/bill_summaries_all)

Returns a list of summaries sorted by date of last update.

GET[/summaries/{congress}](https://gpo.congress.gov/#/summaries/bill_summaries_by_congress)

Returns a list of summaries filtered by congress, sorted by date of last update.

GET[/summaries/{congress}/{billType}](https://gpo.congress.gov/#/summaries/bill_summaries_by_type)

Returns a list of summaries filtered by congress and by bill type, sorted by date of last update.

#### [congress](https://gpo.congress.gov/\#/congress)     Returns congress and congressional sessions data from the API

GET[/congress](https://gpo.congress.gov/#/congress/congress_list1)

Returns a list of congresses and congressional sessions.

GET[/congress/{congress}](https://gpo.congress.gov/#/congress/congress_details)

Returns detailed information for a specified congress.

GET[/congress/current](https://gpo.congress.gov/#/congress/congress_current_list)

Returns detailed information for the current congress.

#### [member](https://gpo.congress.gov/\#/member)     Returns member data from the API

GET[/member](https://gpo.congress.gov/#/member/member_list)

Returns a list of congressional members.

GET[/member/{bioguideId}](https://gpo.congress.gov/#/member/member_details)

Returns detailed information for a specified congressional member.

GET[/member/{bioguideId}/sponsored-legislation](https://gpo.congress.gov/#/member/sponsorship_list)

Returns the list of legislation sponsored by a specified congressional member.

GET[/member/{bioguideId}/cosponsored-legislation](https://gpo.congress.gov/#/member/cosponsorship_list)

Returns the list of legislation cosponsored by a specified congressional member.

GET[/member/congress/{congress}](https://gpo.congress.gov/#/member/congress_list2)

Returns the list of members specified by Congress

GET[/member/{stateCode}](https://gpo.congress.gov/#/member/member_list_by_state)

Returns a list of members filtered by state.

GET[/member/{stateCode}/{district}](https://gpo.congress.gov/#/member/member_list_by_state_and_district)

Returns a list of members filtered by state and district.

GET[/member/congress/{congress}/{stateCode}/{district}](https://gpo.congress.gov/#/member/member_list_by_congress_state_district)

Returns a list of members filtered by congress, state and district.

#### [committee](https://gpo.congress.gov/\#/committee)     Returns committee data from the API

GET[/committee](https://gpo.congress.gov/#/committee/committee_list)

Returns a list of congressional committees.

GET[/committee/{chamber}](https://gpo.congress.gov/#/committee/committee_list_by_chamber)

Returns a list of congressional committees filtered by the specified chamber.

GET[/committee/{congress}](https://gpo.congress.gov/#/committee/committee_list_by_congress)

Returns a list of congressional committees filtered by the specified congress.

GET[/committee/{congress}/{chamber}](https://gpo.congress.gov/#/committee/committee_list_by_congress_chamber)

Returns a list of committees filtered by the specified congress and chamber.

GET[/committee/{chamber}/{committeeCode}](https://gpo.congress.gov/#/committee/committee_details)

Returns detailed information for a specified congressional committee.

GET[/committee/{chamber}/{committeeCode}/bills](https://gpo.congress.gov/#/committee/committee_bills_list)

Returns the list of legislation associated with the specified congressional committee.

GET[/committee/{chamber}/{committeeCode}/reports](https://gpo.congress.gov/#/committee/committee_reports_by_committee)

Returns the list of committee reports associated with a specified congressional committee.

GET[/committee/{chamber}/{committeeCode}/nominations](https://gpo.congress.gov/#/committee/nomination_by_committee)

Returns the list of nominations associated with a specified congressional committee.

GET[/committee/{chamber}/{committeeCode}/house-communication](https://gpo.congress.gov/#/committee/house_communications_by_committee)

Returns the list of House communications associated with a specified congressional committee.

GET[/committee/{chamber}/{committeeCode}/senate-communication](https://gpo.congress.gov/#/committee/senate_communications_by_committee)

Returns the list of Senate communications associated with a specified congressional committee.

#### [committee-report](https://gpo.congress.gov/\#/committee-report)     Returns committee report data from the API

GET[/committee-report](https://gpo.congress.gov/#/committee-report/committee_reports)

Returns a list of committee reports.

GET[/committee-report/{congress}](https://gpo.congress.gov/#/committee-report/committee_reports_by_congress)

Returns a list of committee reports filtered by the specified congress.

GET[/committee-report/{congress}/{reportType}](https://gpo.congress.gov/#/committee-report/committee_reports_by_congress_rpt_type)

Returns a list of committee reports filtered by the specified congress and report type.

GET[/committee-report/{congress}/{reportType}/{reportNumber}](https://gpo.congress.gov/#/committee-report/committee_report_details)

Returns detailed information for a specified committee report.

GET[/committee-report/{congress}/{reportType}/{reportNumber}/text](https://gpo.congress.gov/#/committee-report/committee_report_id_text)

Returns the list of texts for a specified committee report.

#### [committee-print](https://gpo.congress.gov/\#/committee-print)     Returns committee print data from the API

GET[/committee-print](https://gpo.congress.gov/#/committee-print/committee_print_list)

Returns a list of committee prints.

GET[/committee-print/{congress}](https://gpo.congress.gov/#/committee-print/committee_prints_by_congress)

Returns a list of committee prints filtered by the specified congress.

GET[/committee-print/{congress}/{chamber}](https://gpo.congress.gov/#/committee-print/committee_prints_by_congress_chamber)

Returns a list of committee prints filtered by the specified congress and chamber.

GET[/committee-print/{congress}/{chamber}/{jacketNumber}](https://gpo.congress.gov/#/committee-print/committee_print_detail)

Returns detailed information for a specified committee print.

GET[/committee-print/{congress}/{chamber}/{jacketNumber}/text](https://gpo.congress.gov/#/committee-print/committee_print_text)

Returns the list of texts for a specified committee print.

#### [committee-meeting](https://gpo.congress.gov/\#/committee-meeting)     Returns committee meeting data from the API

GET[/committee-meeting](https://gpo.congress.gov/#/committee-meeting/committee_meeting_list)

Returns a list of committee meetings.

GET[/committee-meeting/{congress}](https://gpo.congress.gov/#/committee-meeting/committee_meeting_congress)

Returns a list of committee meetings filtered by the specified congress.

GET[/committee-meeting/{congress}/{chamber}](https://gpo.congress.gov/#/committee-meeting/committee_meeting_congress_chamber)

Returns a list of committee meetings filtered by the specified congress and chamber.

GET[/committee-meeting/{congress}/{chamber}/{eventId}](https://gpo.congress.gov/#/committee-meeting/committee_meeting_detail)

Returns detailed information for a specified committee meeting.

#### [hearing](https://gpo.congress.gov/\#/hearing)     Returns hearing data from the API

GET[/hearing](https://gpo.congress.gov/#/hearing/hearing_list)

Returns a list of hearings.

GET[/hearing/{congress}](https://gpo.congress.gov/#/hearing/hearing_list_by_congress)

Returns a list of hearings filtered by the specified congress.

GET[/hearing/{congress}/{chamber}](https://gpo.congress.gov/#/hearing/hearing_list_by_congress_chamber)

Returns a list of hearings filtered by the specified congress and chamber.

GET[/hearing/{congress}/{chamber}/{jacketNumber}](https://gpo.congress.gov/#/hearing/hearing_detail)

Returns detailed information for a specified hearing.

#### [congressional-record](https://gpo.congress.gov/\#/congressional-record)     Returns Congressional Record data from the API

GET[/congressional-record](https://gpo.congress.gov/#/congressional-record/congressional_record_list)

Returns a list of congressional record issues sorted by most recent.

#### [daily-congressional-record](https://gpo.congress.gov/\#/daily-congressional-record)     Returns daily Congressional Record data from the API

GET[/daily-congressional-record](https://gpo.congress.gov/#/daily-congressional-record/daily_congressional_record_list)

Returns a list of daily congressional record issues sorted by most recent.

GET[/daily-congressional-record/{volumeNumber}](https://gpo.congress.gov/#/daily-congressional-record/daily_congressional_record_list_by_volume)

Returns a list of daily Congressional Records filtered by the specified volume number.

GET[/daily-congressional-record/{volumeNumber}/{issueNumber}](https://gpo.congress.gov/#/daily-congressional-record/daily_congressional_record_list_by_volume_and_issue)

Returns a list of daily Congressional Records filtered by the specified volume number and specified issue number.

GET[/daily-congressional-record/{volumeNumber}/{issueNumber}/articles](https://gpo.congress.gov/#/daily-congressional-record/daily_congressional_record_list_by_article)

Returns a list of daily Congressional Record articles filtered by the specified volume number and specified issue number.

#### [bound-congressional-record](https://gpo.congress.gov/\#/bound-congressional-record)     Returns bound Congressional Record data from the API

GET[/bound-congressional-record](https://gpo.congress.gov/#/bound-congressional-record/bound_congressional_record_list)

Returns a list of bound Congressional Records sorted by most recent.

GET[/bound-congressional-record/{year}](https://gpo.congress.gov/#/bound-congressional-record/bound_congressional_record_list_by_year)

Returns a list of bound Congressional Records filtered by the specified year.

GET[/bound-congressional-record/{year}/{month}](https://gpo.congress.gov/#/bound-congressional-record/bound_congressional_record_list_by_year_and_month)

Returns a list of bound Congressional Records filtered by the specified year and specified month.

GET[/bound-congressional-record/{year}/{month}/{day}](https://gpo.congress.gov/#/bound-congressional-record/bound_congressional_record_list_by_year_and_month_and_day)

Returns a list of bound Congressional Records filtered by the specified year, specified month and specified day.

#### [house-communication](https://gpo.congress.gov/\#/house-communication)     Returns House communication data from the API

GET[/house-communication](https://gpo.congress.gov/#/house-communication/house_communication)

Returns a list of House communications.

GET[/house-communication/{congress}](https://gpo.congress.gov/#/house-communication/house_communication_congress)

Returns a list of House communications filtered by the specified congress.

GET[/house-communication/{congress}/{communicationType}](https://gpo.congress.gov/#/house-communication/house_communication_list)

Returns a list of House communications filtered by the specified congress and communication type.

GET[/house-communication/{congress}/{communicationType}/{communicationNumber}](https://gpo.congress.gov/#/house-communication/house_communication_detail)

Returns detailed information for a specified House communication.

#### [house-requirement](https://gpo.congress.gov/\#/house-requirement)     Returns House requirement data from the API

GET[/house-requirement](https://gpo.congress.gov/#/house-requirement/house_requirement)

Returns a list of House requirements.

GET[/house-requirement/{requirementNumber}](https://gpo.congress.gov/#/house-requirement/house_requirement_detail)

Returns detailed information for a specified House requirement.

GET[/house-requirement/{requirementNumber}/matching-communications](https://gpo.congress.gov/#/house-requirement/house_requirement_communication_list)

Returns a list of matching communications to a House requirement.

#### [senate-communication](https://gpo.congress.gov/\#/senate-communication)     Returns Senate communication data from the API

GET[/senate-communication](https://gpo.congress.gov/#/senate-communication/senate_communication)

Returns a list of Senate communications.

GET[/senate-communication/{congress}](https://gpo.congress.gov/#/senate-communication/senate_communication_congress)

Returns a list of Senate communications filtered by the specified congress.

GET[/senate-communication/{congress}/{communicationType}](https://gpo.congress.gov/#/senate-communication/senate_communication_list)

Returns a list of Senate communications filtered by the specified congress and communication type.

GET[/senate-communication/{congress}/{communicationType}/{communicationNumber}](https://gpo.congress.gov/#/senate-communication/senate_communication_detail)

Returns detailed information for a specified Senate communication.

#### [nomination](https://gpo.congress.gov/\#/nomination)     Returns nomination data from the API

GET[/nomination](https://gpo.congress.gov/#/nomination/nomination_list)

Returns a list of nominations sorted by date received from the President.

GET[/nomination/{congress}](https://gpo.congress.gov/#/nomination/nomination_list_by_congress)

Returns a list of nominations filtered by the specified congress and sorted by date received from the President.

GET[/nomination/{congress}/{nominationNumber}](https://gpo.congress.gov/#/nomination/nomination_detail)

Returns detailed information for a specified nomination.

GET[/nomination/{congress}/{nominationNumber}/{ordinal}](https://gpo.congress.gov/#/nomination/nominees)

Returns the list nominees for a position within the nomination.

GET[/nomination/{congress}/{nominationNumber}/actions](https://gpo.congress.gov/#/nomination/nomination_actions)

Returns the list of actions on a specified nomination.

GET[/nomination/{congress}/{nominationNumber}/committees](https://gpo.congress.gov/#/nomination/nomination_committees)

Returns the list of committees associated with a specified nomination.

GET[/nomination/{congress}/{nominationNumber}/hearings](https://gpo.congress.gov/#/nomination/nomination_hearings)

Returns the list of printed hearings associated with a specified nomination.

#### [treaty](https://gpo.congress.gov/\#/treaty)     Returns treaty data from the API

GET[/treaty](https://gpo.congress.gov/#/treaty/treaty_list)

Returns a list of treaties sorted by date of last update.

GET[/treaty/{congress}](https://gpo.congress.gov/#/treaty/treaty_list_by_congress)

Returns a list of treaties for the specified congress, sorted by date of last update.

GET[/treaty/{congress}/{treatyNumber}](https://gpo.congress.gov/#/treaty/treaty_detail)

Returns detailed information for a specified treaty.

GET[/treaty/{congress}/{treatyNumber}/{treatySuffix}](https://gpo.congress.gov/#/treaty/treaty_details)

Returns detailed information for a specified partitioned treaty.

GET[/treaty/{congress}/{treatyNumber}/actions](https://gpo.congress.gov/#/treaty/treaty_action)

Returns the list of actions on a specified treaty.

GET[/treaty/{congress}/{treatyNumber}/{treatySuffix}/actions](https://gpo.congress.gov/#/treaty/treaty_actions)

Returns the list of actions on a specified partitioned treaty.

GET[/treaty/{congress}/{treatyNumber}/committees](https://gpo.congress.gov/#/treaty/treaty_committee)

Returns the list of committees associated with a specified treaty.