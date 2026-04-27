INSERT INTO `prj-ai-dev-qic.governance.analytical_insights_cache` 
(insight_date, domain, dataset_id, table_id, insight_type, insight_text, generated_at)
VALUES
  -- ==========================================
  -- DECEMBER 2025 (31 Days: Winter Kickoff, Exams, Holiday Prep)
  -- ==========================================
  ('2025-12-01', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Launch of the "Winter Wonderland" early-bird campaign. Online sales spiked 15% WoW.', CURRENT_TIMESTAMP()),
  ('2025-12-02', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Routine inspection closed the main parking tram for 3 hours. Minor drop in early morning gate arrivals.', CURRENT_TIMESTAMP()),
  ('2025-12-03', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Start of mid-term exam weeks for local Riyadh schools. Noticed a sharp 22% drop in youth and student ticket categories.', CURRENT_TIMESTAMP()),
  ('2025-12-04', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Unseasonably cold winds in the evening. Night-pass sales dropped by 30% compared to average Thursdays.', CURRENT_TIMESTAMP()),
  ('2025-12-05', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Heavy morning rainfall. Walk-in gate sales dropped 60%; multiple outdoor rides suspended until 14:00.', CURRENT_TIMESTAMP()),
  ('2025-12-06', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Mobile App crashed for iOS users after a bad update push. Online ticket conversion dropped 40% between 10:00 and 15:00.', CURRENT_TIMESTAMP()),
  ('2025-12-07', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'B2B/Corporate', 'SABIC Corporate Family Day. General admission was low, but a single B2B invoice accounted for 8,500 tickets.', CURRENT_TIMESTAMP()),
  ('2025-12-08', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Minor food-poisoning scare at a third-party vendor went viral locally. Ticket cancellations spiked by 8%.', CURRENT_TIMESTAMP()),
  ('2025-12-09', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Major Riyadh Season concert competing for evening demographics. Late-entry tickets dropped 18%.', CURRENT_TIMESTAMP()),
  ('2025-12-10', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'HR/Staffing', 'Flu outbreak among park operations staff. 3 minor rides closed due to understaffing. No major impact on sales, but guest satisfaction scores dipped.', CURRENT_TIMESTAMP()),
  ('2025-12-11', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Ticketing API timeout with major aggregator (Al Matar). Lost an estimated 1,200 third-party bookings.', CURRENT_TIMESTAMP()),
  ('2025-12-12', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Massive TikTok influencer (Noor Stars) visited the park and posted live. Online sales for the weekend surged 210% within 4 hours.', CURRENT_TIMESTAMP()),
  ('2025-12-13', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Record Saturday attendance due to influencer traffic. Reached 98% park capacity. Walk-in sales were halted at 16:30.', CURRENT_TIMESTAMP()),
  ('2025-12-14', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Flash Sale Sunday: 50% off companion tickets. Volume increased, but average ticket yield (ATY) dropped 25%.', CURRENT_TIMESTAMP()),
  ('2025-12-15', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Logistics', 'Traffic accident on Highway 40 caused a 2-hour gridlock. 15% of pre-booked guests requested date changes.', CURRENT_TIMESTAMP()),
  ('2025-12-16', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'End of school exams. Immediate WoW spike of 45% in student group bookings.', CURRENT_TIMESTAMP()),
  ('2025-12-17', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Payment Gateway (Mada) experienced nationwide latency. Checkout abandonment rate hit 65% for two hours.', CURRENT_TIMESTAMP()),
  ('2025-12-18', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Falcons Flight roller coaster closed for scheduled quarterly track alignment. Fast-track pass sales dropped significantly.', CURRENT_TIMESTAMP()),
  ('2025-12-19', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Cross-promotion with AMC Cinemas went live. Users buying Spider-Man tickets got 20% off park entry. Moderate uptick in bundles.', CURRENT_TIMESTAMP()),
  ('2025-12-20', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Perfect 22C sunny weather. Walk-in sales exceeded forecasts by 35%.', CURRENT_TIMESTAMP()),
  ('2025-12-21', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'B2B/Corporate', 'Saudi Aramco Year-End Banquet. Park closed to public until 14:00. Daily revenue high, but public volume metric appears low.', CURRENT_TIMESTAMP()),
  ('2025-12-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Power dip in Sector B caused a 40-minute reset of 5 rides. Compensation vouchers issued, negatively impacting next-day revenue.', CURRENT_TIMESTAMP()),
  ('2025-12-23', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Official start of Saudi Winter School Break. Baseline daily sales volume shifted up by 50% across all channels.', CURRENT_TIMESTAMP()),
  ('2025-12-24', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'A surge in domestic tourism (flights to Riyadh full). Hotel-partner ticket sales doubled.', CURRENT_TIMESTAMP()),
  ('2025-12-25', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Demographics', 'Noticed a 400% spike in Western Expat ticket purchases (aligning with Christmas day off for foreign schools).', CURRENT_TIMESTAMP()),
  ('2025-12-26', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Water main break near main entrance. VIP gate was used for all guests, causing minor entry delays but no sales drop.', CURRENT_TIMESTAMP()),
  ('2025-12-27', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Macroeconomic', 'Monthly government salary deposit day (27th). Immediate 85% spike in premium/VIP ticket purchases.', CURRENT_TIMESTAMP()),
  ('2025-12-28', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Minor sandstorm in the late afternoon. Evening ticket sales plummeted.', CURRENT_TIMESTAMP()),
  ('2025-12-29', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'End-of-year targeted email blast to previous visitors. High conversion rate (8%) driving strong advance bookings.', CURRENT_TIMESTAMP()),
  ('2025-12-30', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Pre-NYE preparations. Some areas cordoned off. Minor complaints but steady sales.', CURRENT_TIMESTAMP()),
  ('2025-12-31', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'New Years Eve Gala. Special premium pricing applied. Total volume lower, but total revenue broke Q4 daily record.', CURRENT_TIMESTAMP()),

  -- ==========================================
  -- JANUARY 2026 (31 Days: Post-Holiday Lull, Maintenance, Cold Snaps)
  -- ==========================================
  ('2026-01-01', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'New Years Day. Very slow morning gate sales (hangover effect), picking up heavily after 15:00.', CURRENT_TIMESTAMP()),
  ('2026-01-02', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Post-holiday deep cleaning. Park capacity artificially capped at 70%.', CURRENT_TIMESTAMP()),
  ('2026-01-03', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Last weekend of winter break. Massive FOMO (Fear Of Missing Out) spike; sold out by 13:00.', CURRENT_TIMESTAMP()),
  ('2026-01-04', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Back to school/work day. Expected massive drop in volume. Baseline shifted down 60% from previous week.', CURRENT_TIMESTAMP()),
  ('2026-01-05', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Initiated "Jan-Slump" BOGO (Buy One Get One) offer for weekdays. Volume stabilized.', CURRENT_TIMESTAMP()),
  ('2026-01-06', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Main CRM system sync failure. Loyalty points could not be redeemed, causing frustration and a 5% drop in gate walk-ins.', CURRENT_TIMESTAMP()),
  ('2026-01-07', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Severe cold snap (8C). Outdoor water-based rides closed entirely. 20% drop in overall sales.', CURRENT_TIMESTAMP()),
  ('2026-01-08', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'HR/Staffing', 'Annual corporate offsite for Qiddiya upper management. No impact on operational ticket sales.', CURRENT_TIMESTAMP()),
  ('2026-01-09', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Launched local radio ad campaign. Hard to track direct attribution, but Friday sales beat forecast by 10%.', CURRENT_TIMESTAMP()),
  ('2026-01-10', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Logistics', 'Metro red-line extension to Qiddiya experienced delays. 500+ guests requested refunds due to transit failure.', CURRENT_TIMESTAMP()),
  ('2026-01-11', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Sirocco Tower emergency sensor replacement. Ride down all day. Fast-track sales paused to manage expectations.', CURRENT_TIMESTAMP()),
  ('2026-01-12', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Major international football match (Supercopa) hosted in Riyadh. Evening ticket sales basically flatlined as locals watched the game.', CURRENT_TIMESTAMP()),
  ('2026-01-13', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'B2B/Corporate', 'Ministry of Education bulk purchase for high-achieving students. +4,000 tickets injected into Tuesday metrics.', CURRENT_TIMESTAMP()),
  ('2026-01-14', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'AWS Middle East regional outage. Our ticketing website was down for 6 hours. Massive revenue loss.', CURRENT_TIMESTAMP()),
  ('2026-01-15', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Apology promo sent out due to AWS outage (30% off). Recouped 80% of lost revenue in a single day.', CURRENT_TIMESTAMP()),
  ('2026-01-16', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Perfect winter weekend. Sales normalized and hit exact baseline forecasts.', CURRENT_TIMESTAMP()),
  ('2026-01-17', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Sponsored a major e-sports tournament. Slight uptick in teenage demographic ticket purchases.', CURRENT_TIMESTAMP()),
  ('2026-01-18', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Introduced new AI-powered security scanners. Entry bottlenecked for 2 hours, resulting in bad social media sentiment, but sales were already processed.', CURRENT_TIMESTAMP()),
  ('2026-01-19', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Start of the global "World Economic Forum" style event in Riyadh. Hotels full of diplomats, domestic tourism down.', CURRENT_TIMESTAMP()),
  ('2026-01-20', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Food supply chain issue. 3 major restaurants closed. Guest duration in park dropped, but initial ticket sales unaffected.', CURRENT_TIMESTAMP()),
  ('2026-01-21', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Apple Pay integration failed on the mobile app. Users forced to use physical cards. Conversion rate dropped 12%.', CURRENT_TIMESTAMP()),
  ('2026-01-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Launch of the "Qiddiya Resident Pass" for locals. Subscription revenue spiked, daily single-ticket sales began to cannibalize.', CURRENT_TIMESTAMP()),
  ('2026-01-23', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Extreme fog warning. Visibility extremely low. Park voluntarily closed 3 hours early for safety.', CURRENT_TIMESTAMP()),
  ('2026-01-24', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Logistics', 'Due to previous day fog, massive backlog of rescheduled tickets used today. Appears as a revenue spike, but cash was recognized earlier.', CURRENT_TIMESTAMP()),
  ('2026-01-25', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'HR/Staffing', 'Mass onboarding day for 200 new park staff. Minor operational inefficiencies noticed, but no sales impact.', CURRENT_TIMESTAMP()),
  ('2026-01-26', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Riyadh Marathon closed major roads around the city. Qiddiya accessibility severely limited. 30% drop in walk-ins.', CURRENT_TIMESTAMP()),
  ('2026-01-27', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Macroeconomic', 'Government Payday. Expected monthly surge in VIP and Fast-Track upgrades materialized perfectly.', CURRENT_TIMESTAMP()),
  ('2026-01-28', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Turnstile barcode scanners went offline. Staff had to manually check receipts. Huge queues, but revenue secure.', CURRENT_TIMESTAMP()),
  ('2026-01-29', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Collaborated with Saudi Airlines for boarding pass discounts. Noticeable increase in out-of-town guest zip codes.', CURRENT_TIMESTAMP()),
  ('2026-01-30', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'First "Night Concert" inside Six Flags featuring a local DJ. Evening ticket sales broke Q1 records.', CURRENT_TIMESTAMP()),
  ('2026-01-31', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'End of month reconciliation. Standard baseline weekend sales.', CURRENT_TIMESTAMP()),

  -- ==========================================
  -- FEBRUARY 2026 (28 Days: Foundation Day, Mild Spring, Ramadan Prep)
  -- ==========================================
  ('2026-02-01', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Dynamic pricing algorithm adjusted weekend baseline up by 5% due to high historical demand. No drop in volume observed.', CURRENT_TIMESTAMP()),
  ('2026-02-02', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Routine closure of the kids zone for painting. Family ticket packages dropped 15%.', CURRENT_TIMESTAMP()),
  ('2026-02-03', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Beautiful 24C spring weather. Highest Tuesday walk-in conversion rate of the year so far.', CURRENT_TIMESTAMP()),
  ('2026-02-04', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'B2B/Corporate', 'STC Corporate offsite buyout. 12,000 guaranteed attendees. Public sales paused for the day.', CURRENT_TIMESTAMP()),
  ('2026-02-05', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Database lock on the backend ticketing server. 15-minute outage, negligible impact on daily totals.', CURRENT_TIMESTAMP()),
  ('2026-02-06', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Start of the LEAP Tech Conference in Riyadh. Massive influx of business tourists. High evening pass sales.', CURRENT_TIMESTAMP()),
  ('2026-02-07', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'LEAP Conference VIPs given free access lanyards. Entry volume high, but actual ticket revenue low.', CURRENT_TIMESTAMP()),
  ('2026-02-08', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Added 3 new food trucks near the rollercoasters. Increased dwell time, leading to higher retail sales, though tickets remained static.', CURRENT_TIMESTAMP()),
  ('2026-02-09', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Logistics', 'Sandstorm warning caused proactive cancellation of all school trips for the week. Sharp drop in group sales.', CURRENT_TIMESTAMP()),
  ('2026-02-10', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Actual sandstorm hit. Park operational but outdoor visibility zero. Massive 75% drop in daily revenue.', CURRENT_TIMESTAMP()),
  ('2026-02-11', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Post-sandstorm cleanup. Park opened 2 hours late. 15% revenue loss for the day.', CURRENT_TIMESTAMP()),
  ('2026-02-12', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Pre-Foundation Day hype campaign launched on Snapchat. Advance bookings for the 22nd surged.', CURRENT_TIMESTAMP()),
  ('2026-02-13', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Local competitor (Boulevard World) hosted a massive free-entry day. Qiddiya weekend sales took a rare 20% hit.', CURRENT_TIMESTAMP()),
  ('2026-02-14', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Unannounced "2-for-1" flash sale to combat competitor traffic. Successfully pulled volume back to baseline.', CURRENT_TIMESTAMP()),
  ('2026-02-15', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'HR/Staffing', 'Union/contractor dispute with security staff provider. Minor delays at bag check, no sales impact.', CURRENT_TIMESTAMP()),
  ('2026-02-16', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Fraud detection AI aggressively blocked 400 legitimate credit card transactions. 5% drop in daily online revenue.', CURRENT_TIMESTAMP()),
  ('2026-02-17', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Pre-Ramadan shopping weeks begin. Retail malls see spikes, entertainment venues see slight WoW dips (-8%).', CURRENT_TIMESTAMP()),
  ('2026-02-18', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Fire alarm triggered in Sector C (false positive). Park evacuated for 1 hour. Full refunds issued to 1,500 guests.', CURRENT_TIMESTAMP()),
  ('2026-02-19', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 'Launch of the "Foundation Day VIP Package". Extremely high conversion on premium tiers.', CURRENT_TIMESTAMP()),
  ('2026-02-20', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 'Mild rain shower. Actually drove traffic UP as locals enjoy the rare rain. +12% walk-in spike.', CURRENT_TIMESTAMP()),
  ('2026-02-21', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Foundation Day Eve. Huge influx of domestic travelers into Riyadh. Hotel-bundled tickets maxed out.', CURRENT_TIMESTAMP()),
  ('2026-02-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Saudi Foundation Day. Public Holiday. Park reached maximum capacity by 11:30 AM. Largest single revenue day of Q1.', CURRENT_TIMESTAMP()),
  ('2026-02-23', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 'Foundation Day long weekend continues. Sustained maximum capacity. Total sell-out.', CURRENT_TIMESTAMP()),
  ('2026-02-24', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 'Heavy strain on park infrastructure after 3 days of max capacity. 4 minor rides down for emergency repair. Sales still high.', CURRENT_TIMESTAMP()),
  ('2026-02-25', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 'Post-holiday exhaustion. Massive 60% drop in volume as people return to work and school.', CURRENT_TIMESTAMP()),
  ('2026-02-26', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Macroeconomic', 'February Payday (27th falls on Friday, paid early). Slight bump in evening sales.', CURRENT_TIMESTAMP()),
  ('2026-02-27', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 'Ticketing database maintenance window extended into operating hours. Walk-in gates delayed by 45 mins.', CURRENT_TIMESTAMP()),
  ('2026-02-28', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 'Final day of Q1. Aggressive discounts pushed to meet quarterly targets. High volume, low yield.', CURRENT_TIMESTAMP());



INSERT INTO `prj-ai-dev-qic.governance.analytical_insights_cache` 
(insight_date, domain, dataset_id, table_id, insight_type, insight_text, generated_at)
VALUES
  -- ==========================================
  -- DECEMBER 2025: Compounding Holiday Scenarios
  -- ==========================================
  -- Scenario: The "Viral Failure" (Marketing success killed by IT)
  ('2025-12-12', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Marketing', 
   'Massive TikTok influencer (Noor Stars) visited the park. Caused a 500% spike in concurrent web traffic.', CURRENT_TIMESTAMP()),
  ('2025-12-12', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 
   'Due to the influencer traffic spike, the AWS load balancers failed. Web sales were down for 3 hours during peak evening booking time. High cart abandonment.', CURRENT_TIMESTAMP()),

  -- Scenario: The "Silver Lining" (Bad weather, but good strategic pivot)
  ('2025-12-05', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 
   'Unexpected heavy rainfall in Riyadh. Walk-in gate sales for the outdoor park dropped 60%.', CURRENT_TIMESTAMP()),
  ('2025-12-05', 'Entertainment', 'qic_asset_amc', 'fact_cinema_ticket_sales', 'Operations', 
   'Due to the rain, Qiddiya AMC Cinemas saw a 140% surge in walk-in traffic as guests sought indoor entertainment. Total cross-domain revenue remained stable.', CURRENT_TIMESTAMP()),

  -- Scenario: Operations & HR collision
  ('2025-12-24', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 
   'Start of Saudi Winter School Break. Expected a 45% increase in baseline volume.', CURRENT_TIMESTAMP()),
  ('2025-12-24', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'HR/Staffing', 
   'Severe flu outbreak among third-party security contractors. 4 out of 10 security lanes closed. Caused massive 2-hour queues at the gate, resulting in 300+ immediate refund requests.', CURRENT_TIMESTAMP()),

  -- ==========================================
  -- JANUARY 2026: Macro, Logistics, and Competitors
  -- ==========================================
  -- Scenario: External Events impacting Logistics
  ('2026-01-19', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 
   'Global Tech Expo (LEAP preview) began in Riyadh. Hotels hit 99% occupancy, driving massive tourist volume.', CURRENT_TIMESTAMP()),
  ('2026-01-19', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Logistics', 
   'Due to VIP motorcades for the Tech Expo, Highway 40 to Qiddiya experienced unprecedented gridlock. 22% of pre-booked afternoon guests never arrived (No-Shows).', CURRENT_TIMESTAMP()),

  -- Scenario: Pricing War & Retaliation
  ('2026-01-26', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'External Factor', 
   'Local competitor "Boulevard World" launched a surprise "Free Entry Week". Qiddiya morning sales instantly plummeted by 35%.', CURRENT_TIMESTAMP()),
  ('2026-01-26', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Pricing Strategy', 
   'In retaliation to competitor pricing, Qiddiya ops triggered an emergency 50% off push-notification to the CRM database at 13:00. Volume recovered by evening, but overall yield was destroyed.', CURRENT_TIMESTAMP()),

  -- Scenario: The "Payday Premium" vs. Maintenance
  ('2026-01-27', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Macroeconomic', 
   'Government Salary Day. Typical 80% spike in VIP and Fast-Track passes expected.', CURRENT_TIMESTAMP()),
  ('2026-01-27', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 
   'Emergency sensor failure on Falcons Flight (flagship coaster). Ride closed all day. The VIP/Fast-Track guests demanded partial refunds, negating the Payday revenue bump.', CURRENT_TIMESTAMP()),

  -- ==========================================
  -- FEBRUARY 2026: Foundation Day Chaos
  -- ==========================================
  -- Scenario: Extreme Weather vs Indoor Retail
  ('2026-02-10', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Weather Event', 
   'Massive Sandstorm (Red Alert). Outdoor rides legally mandated to shut down. Park attendance dropped to near-zero by 14:00.', CURRENT_TIMESTAMP()),
  ('2026-02-10', 'Retail', 'qic_shared', 'fact_merchandise_sales', 'Consumer Behavior', 
   'Guests trapped in the park during the sandstorm sheltered in the main retail promenade. Merchandise revenue per capita spiked 400% despite low ticket sales.', CURRENT_TIMESTAMP()),

  -- Scenario: The "Too Successful" Foundation Day
  ('2026-02-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Calendar Event', 
   'Saudi Foundation Day public holiday. Record-breaking demand. Park hit maximum legal capacity (100%) by 11:15 AM.', CURRENT_TIMESTAMP()),
  ('2026-02-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Operations', 
   'Due to max capacity, gates were locked. Over 5,000 angry walk-up guests were turned away at the parking lot. Social media sentiment crashed, but ticket revenue was capped at maximum.', CURRENT_TIMESTAMP()),
  ('2026-02-22', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Supply Chain', 
   'Because of the unprecedented crowd density, 3 major F&B kiosks ran completely out of food inventory by 16:00, leading to lost secondary revenue.', CURRENT_TIMESTAMP()),

  -- Scenario: System Fraud False Positive
  ('2026-02-16', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'System Outage', 
   'Our new AI Fraud Detection gateway malfunctioned, flagging all international credit cards as high-risk.', CURRENT_TIMESTAMP()),
  ('2026-02-16', 'Theme Parks', 'qic_asset_sixflags', 'fact_ticket_sales', 'Demographics', 
   'Because of the gateway error, sales to Western and Asian expats dropped by 92% for the day. Local Mada card sales were unaffected.', CURRENT_TIMESTAMP());
















#########################################################




-- 1. Create/Ensure the Causal Matrix exists
CREATE OR REPLACE TABLE `prj-ai-dev-qic.governance.causal_governance_matrix` (
    dependent_dataset STRING,
    dependent_table STRING,
    driver_dataset STRING,
    driver_table STRING,
    join_keys STRING,
    relationship_description STRING
);

-- 2. Insert the Cross-Domain Join Rules
INSERT INTO `prj-ai-dev-qic.governance.causal_governance_matrix`
(dependent_dataset, dependent_table, driver_dataset, driver_table, join_keys, relationship_description)
VALUES
  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_shared', 'dim_enterprise_calendar', 
   'DATE(primary.timestamp) = driver.calendar_date', 
   'Link sales drops/spikes to weather, holidays, payday, or external competitor events.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_it_ops', 'fact_system_downtime', 
   'DATE(primary.timestamp) = driver.incident_date', 
   'Check if sales drops correlate with POS, AWS, or Payment Gateway outages.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_marketing', 'fact_campaign_spend', 
   'DATE(primary.timestamp) = driver.campaign_date', 
   'Identify if sudden spikes/drops are driven by marketing campaigns or emergency pricing promos.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_hr', 'fact_staffing_levels', 
   'DATE(primary.timestamp) = driver.shift_date', 
   'Check if sales/throughput drops are caused by severe staff shortages or illnesses.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_logistics', 'fact_traffic_incidents', 
   'DATE(primary.timestamp) = driver.incident_date', 
   'Check if no-shows or ticket cancellations correlate with highway closures or traffic gridlock.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_asset_sixflags', 'fact_ride_maintenance', 
   'DATE(primary.timestamp) = driver.maintenance_date', 
   'Correlate VIP pass refund spikes with flagship ride breakdowns.'),

  ('qic_asset_sixflags', 'fact_ticket_sales', 'qic_supply_chain', 'fact_inventory_stockouts', 
   'DATE(primary.timestamp) = driver.stockout_date', 
   'Check if max capacity days resulted in F&B inventory failures.');







CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_shared.dim_enterprise_calendar` (
    calendar_date DATE,
    day_type STRING,
    weather_alert STRING,
    macro_event STRING,
    competitor_activity STRING
);

INSERT INTO `prj-ai-dev-qic.qic_shared.dim_enterprise_calendar`
(calendar_date, day_type, weather_alert, macro_event, competitor_activity)
VALUES
  ('2025-12-05', 'Standard Weekday', 'Heavy Rain (Red Alert)', 'None', 'None'),
  ('2025-12-24', 'School Holiday', 'Clear', 'Winter Break Start', 'None'),
  ('2026-01-19', 'Standard Weekday', 'Clear', 'LEAP Tech Expo (City-wide hotel sellout)', 'None'),
  ('2026-01-26', 'Standard Weekday', 'Clear', 'None', 'Boulevard World Free Entry Week'),
  ('2026-01-27', 'Standard Weekday', 'Clear', 'Government Salary Deposit Day', 'None'),
  ('2026-02-10', 'Standard Weekday', 'Sandstorm (Visibility Zero)', 'None', 'None'),
  ('2026-02-22', 'Public Holiday', 'Clear', 'Saudi Foundation Day', 'None');

CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_it_ops.fact_system_downtime` (
    incident_date DATE,
    system_impacted STRING,
    downtime_minutes INT64,
    root_cause STRING
);

INSERT INTO `prj-ai-dev-qic.qic_it_ops.fact_system_downtime`
(incident_date, system_impacted, downtime_minutes, root_cause)
VALUES
  ('2025-12-12', 'AWS Load Balancer / Web Gateway', 180, 'Traffic surge from social media caused memory overflow.'),
  ('2026-02-16', 'Payment Gateway (Fraud Detection)', 360, 'AI False Positive blocked 92% of international IP addresses.');

CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_marketing.fact_campaign_spend` (
    campaign_date DATE,
    campaign_name STRING,
    channel STRING,
    discount_offered FLOAT64
);

INSERT INTO `prj-ai-dev-qic.qic_marketing.fact_campaign_spend`
(campaign_date, campaign_name, channel, discount_offered)
VALUES
  ('2025-12-12', 'Noor Stars Live Visit', 'TikTok / Influencer', 0.0),
  ('2026-01-26', 'Emergency Competitor Retaliation', 'CRM Push Notification', 0.50);
CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_hr.fact_staffing_levels` (
    shift_date DATE,
    department STRING,
    planned_headcount INT64,
    actual_headcount INT64,
    variance_reason STRING
);

INSERT INTO `prj-ai-dev-qic.qic_hr.fact_staffing_levels`
(shift_date, department, planned_headcount, actual_headcount, variance_reason)
VALUES
  ('2025-12-24', 'Gate Security (3rd Party)', 45, 22, 'Severe flu outbreak; 4 entry lanes permanently closed.');

CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_asset_sixflags.fact_ride_maintenance` (
    maintenance_date DATE,
    ride_name STRING,
    is_flagship_ride BOOLEAN,
    downtime_hours INT64,
    issue_type STRING
);

INSERT INTO `prj-ai-dev-qic.qic_asset_sixflags.fact_ride_maintenance`
(maintenance_date, ride_name, is_flagship_ride, downtime_hours, issue_type)
VALUES
  ('2026-01-27', 'Falcons Flight', TRUE, 12, 'Emergency sensor failure on track sector 4.');


CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_logistics.fact_traffic_incidents` (
    incident_date DATE,
    route_impacted STRING,
    delay_minutes INT64,
    incident_type STRING
);

INSERT INTO `prj-ai-dev-qic.qic_logistics.fact_traffic_incidents`
(incident_date, route_impacted, delay_minutes, incident_type)
VALUES
  ('2026-01-19', 'Highway 40 (Riyadh to Qiddiya)', 150, 'VIP Motorcade Roadblock / Severe Gridlock');

CREATE TABLE IF NOT EXISTS `prj-ai-dev-qic.qic_supply_chain.fact_inventory_stockouts` (
    stockout_date DATE,
    location_id STRING,
    item_category STRING,
    stockout_time TIME
);

INSERT INTO `prj-ai-dev-qic.qic_supply_chain.fact_inventory_stockouts`
(stockout_date, location_id, item_category, stockout_time)
VALUES
  ('2026-02-22', 'Main F&B Plaza', 'Hot Food / Burgers', '15:45:00'),
  ('2026-02-22', 'Waterpark Kiosk 3', 'Bottled Water', '16:10:00'),
  ('2026-02-22', 'Kids Zone Cafe', 'Ice Cream', '14:30:00');



***************************


    INSERT INTO `prj-ai-dev-qic.qic_shared.dim_enterprise_calendar`
(calendar_date, day_type, weather_alert, macro_event, competitor_activity)
VALUES
  -- December 2025
  ('2025-12-16', 'Standard Weekday', 'Clear', 'End of local school exam week', 'None'),
  ('2025-12-20', 'Weekend', 'Perfect Weather (22C, Clear)', 'None', 'None'),
  -- March 2026
  ('2026-03-02', 'Standard Weekday', 'High Winds Alert', 'None', 'None'),
  ('2026-03-03', 'Standard Weekday', 'Severe Sandstorm (Visibility < 100m)', 'None', 'None');

INSERT INTO `prj-ai-dev-qic.qic_it_ops.fact_system_downtime`
(incident_date, system_impacted, downtime_minutes, root_cause)
VALUES
  -- December 2025
  ('2025-12-17', 'Mada Payment Gateway', 120, 'Nationwide latency with banking switch; severe checkout abandonment.'),
  -- March 2026
  ('2026-03-06', 'Ticketing API (Al Matar Integration)', 85, 'API timeout due to sudden surge in weekend promo traffic.');

INSERT INTO `prj-ai-dev-qic.qic_marketing.fact_campaign_spend`
(campaign_date, campaign_name, channel, discount_offered)
VALUES
  -- December 2025
  ('2025-12-14', 'Flash Sale Sunday', 'Email Blast', 0.30),
  ('2025-12-19', 'AMC Cinema Cross-Promotion', 'Partner App Push', 0.20),
  -- March 2026
  ('2026-03-05', 'Post-Storm Weekend Recovery', 'Snapchat Ads', 0.25);

INSERT INTO `prj-ai-dev-qic.qic_hr.fact_staffing_levels`
(shift_date, department, planned_headcount, actual_headcount, variance_reason)
VALUES
  -- March 2026
  ('2026-03-01', 'Ride Operations', 120, 95, 'Post-payday weekend absenteeism; 3 minor family rides operated at half-capacity.');

INSERT INTO `prj-ai-dev-qic.qic_asset_sixflags.fact_ride_maintenance`
(maintenance_date, ride_name, is_flagship_ride, downtime_hours, issue_type)
VALUES
  -- December 2025
  ('2025-12-18', 'Falcons Flight', TRUE, 8, 'Scheduled quarterly track alignment prior to the winter break surge.'),
  -- March 2026
  ('2026-03-04', 'Sirocco Tower', TRUE, 14, 'Emergency cleanup: removing fine grit from mechanical gears following the Mar 3 sandstorm.');


INSERT INTO `prj-ai-dev-qic.qic_logistics.fact_traffic_incidents`
(incident_date, route_impacted, delay_minutes, incident_type)
VALUES
  -- December 2025
  ('2025-12-15', 'Highway 40 (Eastbound to Qiddiya)', 120, 'Major multi-vehicle collision causing 2-hour gridlock for afternoon arrivals.'),
  -- March 2026
  ('2026-03-07', 'Qiddiya Main Interchange', 90, 'Severe weekend bottleneck due to simultaneous events at Six Flags and the nearby Stadium.');


INSERT INTO `prj-ai-dev-qic.qic_supply_chain.fact_inventory_stockouts`
(stockout_date, location_id, item_category, stockout_time)
VALUES
  -- December 2025
  ('2025-12-20', 'Main Retail Hub', 'Winter Merchandise (Hoodies)', '18:30:00');