class CreateCountryStats < ActiveRecord::Migration[5.0]
  def change
    create_table :country_stats do |t|
      t.string :country_code
      t.integer :visit_count
      t.timestamps
    end
  end
end
