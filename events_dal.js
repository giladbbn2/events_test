const { Pool } = require("pg");

const PG_HOST = "localhost";
const PG_USER = "postgres";
const PG_PASSWORD = "qwerty123456";
const PG_DB = "postgres";
const PG_PORT = 5432;

const EventsDal = (function(){
    let pool = null;

    const init = () => {
        pool = new Pool({
            host: PG_HOST,
            user: PG_USER,
            password: PG_PASSWORD,
            database: PG_DB,
            port: PG_PORT
        });
    };

    const getUserRevenue = async (userId) => {
        const { rows } = await pool.query("SELECT revenue FROM users_revenue WHERE user_id = $1", [userId]);

        return new Promise((res, rej) => {
            if (rows.length > 0) {
                res(rows[0]["revenue"]);
            } else {
                res(null);
            }
        });
    };

    const updateUserRevenue = async (userId, addRevenue) => {
        const newAddRevenue = Number(addRevenue);

        const query = {
            text: "INSERT INTO users_revenue (user_id, revenue) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET revenue = users_revenue.revenue + EXCLUDED.revenue",
            values: [userId, newAddRevenue]
        };

        await pool.query(query);
    };

    const closeConnection = () => {
        try {
            pool.end();
        } catch (error) {
            console.error(error);
        }
    };

    return {
        init,
        getUserRevenue,
        updateUserRevenue,
        closeConnection
    }
})();

module.exports = EventsDal;