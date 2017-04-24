class CountryStat < ApplicationRecord
  def self.update_country_counts(country_counts)
    country_counts.each do |country, count|
      next if BLACKLIST.include?(country)
      CountryStat
        .find_or_create_by(country_code: country)
        .increment!(:visit_count, count)
    end
  end

  BLACKLIST = ['Hungary'] # There's a lot of hungary in the data set :-)
end
