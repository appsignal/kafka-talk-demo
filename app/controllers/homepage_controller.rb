class HomepageController < ApplicationController
  def index
    @country_stats = CountryStat.order('visit_count desc')
    @total_visit_count = CountryStat.sum(:visit_count)
    @max_visit_count = CountryStat.maximum(:visit_count)
  end
end
